package integration

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	msdk "github.com/livekit/media-sdk"
	"github.com/livekit/media-sdk/g711"
	"github.com/livekit/media-sdk/rtp"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/sipgo"
	sipmsg "github.com/livekit/sipgo/sip"

	"github.com/livekit/sip/pkg/audiotest"
	"github.com/livekit/sip/pkg/sip"
)

func TestSIPOutboundEarlyMedia(t *testing.T) {
	lk := runLiveKit(t)
	srv := runSIPServer(t, lk)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	const roomName = "early-media"
	observer := lk.ConnectParticipant(t, roomName, "observer", nil)

	ua, err := sipgo.NewUA()
	require.NoError(t, err)
	t.Cleanup(func() { _ = ua.Close() })
	remote, err := sipgo.NewServer(ua)
	require.NoError(t, err)
	lis, err := net.Listen("tcp4", "0.0.0.0:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })
	type invite struct {
		req *sipmsg.Request
		tx  sipmsg.ServerTransaction
	}
	invites := make(chan invite, 1)
	ack := make(chan struct{}, 1)
	bye := make(chan struct{}, 1)
	remote.OnInvite(func(_ *slog.Logger, req *sipmsg.Request, tx sipmsg.ServerTransaction) { invites <- invite{req, tx} })
	remote.OnAck(func(_ *slog.Logger, _ *sipmsg.Request, _ sipmsg.ServerTransaction) {
		select {
		case ack <- struct{}{}:
		default:
		}
	})
	remote.OnBye(func(_ *slog.Logger, req *sipmsg.Request, tx sipmsg.ServerTransaction) {
		_ = tx.Respond(sipmsg.NewResponseFromRequest(req, sipmsg.StatusOK, "OK", nil))
		select {
		case bye <- struct{}{}:
		default:
		}
	})
	go func() { _ = remote.ServeTCP(lis) }()

	trunk := srv.CreateTrunkOut(t, &livekit.SIPOutboundTrunkInfo{
		Name: "early media", Numbers: []string{clientNumber},
		Address:   net.JoinHostPort(srv.IP.String(), strconv.Itoa(lis.Addr().(*net.TCPAddr).Port)),
		Transport: livekit.SIPTransport_SIP_TRANSPORT_TCP,
	})
	t.Cleanup(func() { srv.DeleteTrunk(t, trunk) })
	_, err = srv.Client.CreateSIPParticipant(ctx, &livekit.CreateSIPParticipantRequest{
		SipTrunkId: trunk, SipCallTo: serverNumber, RoomName: roomName,
		ParticipantIdentity: "callee", Dialtone: livekit.SIPDialtone_SIP_DIALTONE_US,
	})
	require.NoError(t, err)
	var in invite
	select {
	case in = <-invites:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	codecs := msdk.NewCodecSet()
	codecs.SetEnabled(g711.ULawSDPNameOnly, true)
	peer, err := sip.NewMediaPort(logger.NewTestLogger(t), nil, &sip.MediaOptions{IP: srv.IP, Codecs: codecs}, sip.RoomSampleRate)
	require.NoError(t, err)
	t.Cleanup(peer.Close)
	answer, err := peer.GenerateAnswer(in.req.Body())
	require.NoError(t, err)
	respond := func(code sipmsg.StatusCode, body []byte) {
		res := sipmsg.NewResponseFromRequest(in.req, code, "", body)
		res.To().Params.Add("tag", "early-callee")
		res.AppendHeader(sipmsg.NewHeader("Content-Type", "application/sdp"))
		require.NoError(t, in.tx.Respond(res))
	}
	respond(sipmsg.StatusRinging, nil)
	respond(sipmsg.StatusSessionInProgress, answer)
	status := func() string {
		if p := observer.Room.GetParticipantByIdentity("callee"); p != nil {
			return p.Attributes()[livekit.AttrSIPCallStatus]
		}
		return ""
	}
	require.Eventually(t, func() bool { return status() == "early_media" }, 5*time.Second, 20*time.Millisecond)
	callee := observer.Room.GetParticipantByIdentity("callee")
	require.Len(t, callee.TrackPublications(), 1)
	trackID := callee.TrackPublications()[0].SID()
	respond(sipmsg.StatusSessionInProgress, answer)
	respond(sipmsg.StatusRinging, nil)
	require.Never(t, func() bool { return status() != "early_media" }, 100*time.Millisecond, 10*time.Millisecond)

	var phase atomic.Int32
	phase.Store(5)
	sendCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	t.Cleanup(func() { stop(); <-done })
	go func() {
		defer close(done)
		ticker := time.NewTicker(rtp.DefFrameDur)
		defer ticker.Stop()
		frame := make(msdk.PCM16Sample, sip.RoomSampleRate/int(time.Second/rtp.DefFrameDur))
		for {
			select {
			case <-sendCtx.Done():
				return
			case <-ticker.C:
			}
			audiotest.GenSignal(frame, []audiotest.Wave{{Ind: int(phase.Load()), Amp: 8191}})
			if err := peer.GetOutboundAudioWriter().WriteSample(frame); err != nil {
				return
			}
		}
	}()
	require.NoError(t, observer.WaitSignals(ctx, []int{5}, nil))
	select {
	case <-peer.Received():
		t.Fatal("room audio reached SIP before answer")
	default:
	}

	// Exercise the SDP fallback while keeping the same LiveKit publication.
	respond(sipmsg.StatusOK, nil)
	select {
	case <-ack:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Eventually(t, func() bool { return status() == "active" }, 5*time.Second, 20*time.Millisecond)
	phase.Store(4)
	require.NoError(t, observer.WaitSignals(ctx, []int{4}, nil))
	require.Len(t, callee.TrackPublications(), 1)
	require.Equal(t, trackID, callee.TrackPublications()[0].SID())
	select {
	case <-peer.Received():
	case <-ctx.Done():
		t.Fatal("room audio did not reach SIP after answer")
	}
	_, err = lk.Rooms.RemoveParticipant(ctx, &livekit.RoomParticipantIdentity{Room: roomName, Identity: "callee"})
	require.NoError(t, err)
	select {
	case <-bye:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
