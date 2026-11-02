package sip

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	msdk "github.com/livekit/media-sdk"
	"github.com/livekit/media-sdk/g711"
	"github.com/livekit/media-sdk/rtp"
	"github.com/livekit/media-sdk/sdp"
	"github.com/livekit/media-sdk/srtp"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/sipgo/sip"

	"github.com/livekit/sip/pkg/audiotest"
)

// The carrier signal is 1600 Hz, distinct from the locally generated ringback.
type earlyAudioCapture struct {
	mu     sync.Mutex
	last   msdk.PCM16Sample
	writes int
	closed atomic.Int32
}

func (w *earlyAudioCapture) String() string  { return "earlyAudioCapture" }
func (w *earlyAudioCapture) SampleRate() int { return RoomSampleRate }
func (w *earlyAudioCapture) Close() error    { w.closed.Add(1); return nil }
func (w *earlyAudioCapture) WriteSample(sample msdk.PCM16Sample) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.last = append(w.last[:0], sample...)
	w.writes++
	return nil
}
func (w *earlyAudioCapture) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writes
}
func (w *earlyAudioCapture) hasCarrierSignal() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.last) == 0 {
		return false
	}
	waves := audiotest.FindSignal(w.last)
	return len(waves) != 0 && waves[0].Ind == 5 && waves[0].Amp > 3000
}

func carrierFrame() msdk.PCM16Sample {
	frame := make(msdk.PCM16Sample, RoomSampleRate/int(time.Second/rtp.DefFrameDur))
	audiotest.GenSignal(frame, []audiotest.Wave{{Ind: 5, Amp: 6000}})
	return frame
}

func requireCarrierAudio(t *testing.T, peer MediaPort, dst *earlyAudioCapture) {
	t.Helper()
	frame := carrierFrame()
	before := dst.count()
	require.Eventually(t, func() bool {
		for range 5 {
			require.NoError(t, peer.GetOutboundAudioWriter().WriteSample(frame))
		}
		return dst.count() > before && dst.hasCarrierSignal()
	}, 3*time.Second, 20*time.Millisecond)
}

func TestMediaPortEarlyAnswer(t *testing.T) {
	for _, encryption := range []sdp.Encryption{sdp.EncryptionNone, sdp.EncryptionRequire} {
		t.Run(fmt.Sprint(encryption), func(t *testing.T) {
			c1, c2 := newUDPPipe()
			log := logger.NewTestLogger(t)
			opts := func(ip string) *MediaOptions {
				return &MediaOptions{IP: newIP(ip), Encryption: encryption, Codecs: testCodecSet(g711.ULawSDPNameOnly, g711.ALawSDPNameOnly)}
			}
			m1 := newTestPort(t, log, c1, opts("1.1.1.1"), RoomSampleRate)
			m2 := newTestPort(t, log, c2, opts("2.2.2.2"), RoomSampleRate)
			offer, err := m1.GenerateOffer()
			require.NoError(t, err)
			answer, err := m2.GenerateAnswer(offer)
			require.NoError(t, err)
			dst := &earlyAudioCapture{}
			m1.WriteInboundAudioTo(dst)

			require.NoError(t, m1.ProcessEarlyAnswer(answer))
			pipeline := m1.pipeline
			require.NotNil(t, m1.offer)
			require.Nil(t, m1.timeoutStart.Load())
			requireCarrierAudio(t, m2, dst)
			require.NoError(t, m1.ProcessEarlyAnswer(answer))
			require.Same(t, pipeline, m1.pipeline, "duplicate 183 must reuse the pipeline")

			// An old early packet must not cause an immediate timeout after answer.
			m1.lastPacketTime.Store(time.Now().Add(-time.Hour).UnixNano())
			require.NoError(t, m1.ProcessAnswer(answer))
			require.Same(t, pipeline, m1.pipeline)
			require.Nil(t, m1.offer)
			require.NotNil(t, m1.timeoutStart.Load())
			select {
			case <-m1.MediaTimeout():
				t.Fatal("early media spent the answered call's timeout")
			case <-time.After(30 * time.Millisecond):
			}
			requireCarrierAudio(t, m2, dst)
			m1.Close()
			require.EqualValues(t, 1, dst.closed.Load())
		})
	}
}

func TestMediaPortEarlyAnswerChanges(t *testing.T) {
	for _, change := range []string{"address", "codec", "payload", "dtmf", "crypto"} {
		t.Run(change, func(t *testing.T) {
			encryption := sdp.EncryptionNone
			if change == "crypto" {
				encryption = sdp.EncryptionRequire
			}
			m := newTestPort(t, logger.NewTestLogger(t), newTestConn(1024), &MediaOptions{
				IP: newIP("1.1.1.1"), Encryption: encryption, Codecs: testCodecSet(g711.ULawSDPNameOnly, g711.ALawSDPNameOnly),
			}, RoomSampleRate)
			offerData, err := m.GenerateOffer()
			require.NoError(t, err)
			answer := func(codec string, port int) []byte {
				offer, err := sdp.ParseOfferWith(testCodecSet(codec), offerData)
				require.NoError(t, err)
				profiles, err := srtp.DefaultProfiles()
				require.NoError(t, err)
				a, _, err := offer.Answer(newIP("2.2.2.2"), port, encryption, sdp.WithLocalProfiles(profiles))
				require.NoError(t, err)
				body, err := a.SDP.Marshal()
				require.NoError(t, err)
				return body
			}
			early := answer(g711.ULawSDPNameOnly, 10000)
			require.NoError(t, m.ProcessEarlyAnswer(early))
			pipeline := m.pipeline
			final := string(early)
			switch change {
			case "address":
				final = strings.ReplaceAll(final, "10000", "10002")
			case "codec":
				final = string(answer(g711.ALawSDPNameOnly, 10000))
			case "payload":
				final = strings.ReplaceAll(final, "RTP/AVP 0", "RTP/AVP 96")
				final = strings.ReplaceAll(final, "rtpmap:0 ", "rtpmap:96 ")
			case "dtmf":
				pt := fmt.Sprint(m.NegotiatedAudio().DTMF.Type)
				final = strings.ReplaceAll(final, " "+pt+"\r\n", " 120\r\n")
				final = strings.ReplaceAll(final, ":"+pt+" ", ":120 ")
			case "crypto":
				final = string(answer(g711.ULawSDPNameOnly, 10000))
			}
			require.NotEqual(t, string(early), final)
			require.NoError(t, m.ProcessAnswer([]byte(final)))
			if change == "address" {
				require.Same(t, pipeline, m.pipeline)
				require.Equal(t, uint16(10002), m.RemoteAddr().Port())
			} else {
				require.NotSame(t, pipeline, m.pipeline)
			}
			require.False(t, m.earlyMedia)
			require.Nil(t, m.offer)
		})
	}
}

func TestMediaPortEarlyAnswerInvalid(t *testing.T) {
	m := newTestPort(t, logger.NewTestLogger(t), newTestConn(1024), &MediaOptions{IP: newIP("1.1.1.1")}, RoomSampleRate)
	offer, err := m.GenerateOffer()
	require.NoError(t, err)
	require.Error(t, m.ProcessEarlyAnswer([]byte("not SDP")))
	require.NotNil(t, m.offer)
	require.Nil(t, m.pipeline)
	require.Nil(t, m.timeoutStart.Load())
	peer := newTestPort(t, logger.NewTestLogger(t), newTestConn(1024), &MediaOptions{IP: newIP("2.2.2.2")}, RoomSampleRate)
	answer, err := peer.GenerateAnswer(offer)
	require.NoError(t, err)
	require.NoError(t, m.ProcessAnswer(answer))
}

type earlyTestRoom struct {
	*testRoom
	audio  *earlyAudioCapture
	tracks atomic.Int32
}

func (r *earlyTestRoom) NewParticipantTrack(int) (msdk.WriteCloser[msdk.PCM16Sample], error) {
	r.tracks.Add(1)
	return r.audio, nil
}

func TestOutboundEarlyMedia(t *testing.T) {
	for _, finalBody := range []bool{true, false} {
		t.Run(fmt.Sprint("final SDP ", finalBody), func(t *testing.T) {
			var room *earlyTestRoom
			h := NewTestSIP(t, TestSIPConfig{GetRoom: func(log logger.Logger, st *RoomStats) RoomInterface {
				room = &earlyTestRoom{testRoom: newTestRoomWithConfig(log, st, nil).(*testRoom), audio: &earlyAudioCapture{}}
				return room
			}})
			req := MinimalCreateSIPParticipantRequest()
			req.Dialtone = livekit.SIPDialtone_SIP_DIALTONE_US
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			_, err := h.Client.CreateSIPParticipant(ctx, req)
			require.NoError(t, err)
			tr := h.WaitTransaction(t, time.Second, req.SipCallId, "")
			h.Client.cmu.Lock()
			call := h.Client.activeCalls[LocalTag(req.SipCallId)]
			h.Client.cmu.Unlock()
			media := getMediaPort(t, call.media)
			peer, err := NewMediaPort(logger.NewTestLogger(t), newTestCallMonitor(t), &MediaOptions{
				IP: netip.MustParseAddr("127.0.0.1"), Codecs: testCodecSet(g711.ULawSDPNameOnly),
			}, RoomSampleRate)
			require.NoError(t, err)
			t.Cleanup(peer.Close)
			answer, err := peer.GenerateAnswer(tr.req.Body())
			require.NoError(t, err)
			send := func(code sip.StatusCode, body []byte) {
				r := sip.NewResponseFromRequest(tr.req, code, "", body)
				r.To().Params.Add("tag", "callee")
				r.AppendHeader(sip.NewHeader("Content-Type", "application/sdp; charset=utf-8"))
				require.NoError(t, tr.transaction.SendResponse(r))
			}
			send(sip.StatusRinging, nil)
			send(sip.StatusSessionInProgress, answer)
			requireCarrierAudio(t, peer, room.audio)
			require.Eventually(t, func() bool { return media.audioIn.Get() != nil }, time.Second, 10*time.Millisecond)
			pipeline := media.pipeline
			writer := media.audioIn.Get()
			require.Nil(t, room.room.outboundAudio.Get())
			require.Nil(t, room.room.outboundDTMF.Get())
			require.False(t, room.room.subscribe.Load())
			require.Nil(t, media.timeoutStart.Load())
			require.EqualValues(t, 1, room.tracks.Load())
			send(sip.StatusSessionInProgress, answer)
			send(sip.StatusRinging, nil)
			// No carrier RTP: the local ringback must not resume or overlap it.
			time.Sleep(60 * time.Millisecond)
			before := room.audio.count()
			time.Sleep(60 * time.Millisecond)
			require.Equal(t, before, room.audio.count())
			body := answer
			if !finalBody {
				body = nil
			}
			send(sip.StatusOK, body)
			ack := h.WaitRequest(t, time.Second, req.SipCallId, "")
			require.Equal(t, sip.ACK, ack.req.Method)
			require.Eventually(t, call.started.IsBroken, time.Second, 10*time.Millisecond)
			require.Same(t, pipeline, media.pipeline)
			require.Same(t, writer, media.audioIn.Get())
			require.EqualValues(t, 1, room.tracks.Load())
			require.NotNil(t, room.room.outboundAudio.Get())
			requireCarrierAudio(t, peer, room.audio)
			closed := make(chan struct{})
			go func() { _ = call.Close(ctx); close(closed) }()
			answerBYE(t, h, time.Second, req.SipCallId)
			<-closed
			require.Eventually(t, func() bool { return room.audio.closed.Load() == 1 }, time.Second, 10*time.Millisecond)
		})
	}
}

func TestSameInviteDialog(t *testing.T) {
	response := func(tag string, seq uint32) *sip.Response {
		r := sip.NewResponse(sip.StatusOK, "OK")
		r.AppendHeader(&sip.ToHeader{Params: sip.HeaderParams{{"tag", tag}}})
		r.AppendHeader(&sip.CSeqHeader{SeqNo: seq, MethodName: sip.INVITE})
		return r
	}
	require.True(t, sameInviteDialog(response("a", 1), response("a", 1)))
	require.False(t, sameInviteDialog(response("a", 1), response("b", 1)))
	require.False(t, sameInviteDialog(response("a", 1), response("a", 2)))
	require.False(t, sameInviteDialog(response("", 1), response("", 1)))
	require.False(t, sameInviteDialog(nil, response("a", 1)))
}

func TestSIPResponseCallbackFailureCancels(t *testing.T) {
	tx := &testSIPClientTransaction{responses: make(chan *sip.Response, 1), cancels: make(chan struct{}, 1), done: make(chan struct{})}
	tx.responses <- sip.NewResponse(sip.StatusSessionInProgress, "Session Progress")
	want := errors.New("media setup failed")
	_, err := sipResponse(t.Context(), tx, nil, func(*sip.Response) error { return want })
	require.ErrorIs(t, err, want)
	require.Len(t, tx.cancels, 1)
}

func TestOutboundEarlyMediaResponses(t *testing.T) {
	for _, scenario := range []string{"no body", "no content type", "wrong content type", "invalid SDP", "incompatible codec", "180 with SDP", "different dialog", "different attempt", "rejected", "cancelled", "cancelled during setup", "ringing timeout", "media failure"} {
		t.Run(scenario, func(t *testing.T) {
			h := NewTestSIP(t, TestSIPConfig{})
			req := MinimalCreateSIPParticipantRequest()
			req.WaitUntilAnswered = true
			if scenario == "ringing timeout" {
				req.RingingTimeout = durationpb.New(200 * time.Millisecond)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := h.Client.CreateSIPParticipant(ctx, req); done <- err }()
			tr := h.WaitTransaction(t, time.Second, req.SipCallId, "")
			h.Client.cmu.Lock()
			call := h.Client.activeCalls[LocalTag(req.SipCallId)]
			h.Client.cmu.Unlock()
			port := getMediaPort(t, call.media)
			peer := newTestPort(t, logger.NewTestLogger(t), newTestConn(1024), &MediaOptions{
				IP: newIP("2.2.2.2"), Codecs: testCodecSet(g711.ULawSDPNameOnly),
			}, RoomSampleRate)
			answer, err := peer.GenerateAnswer(tr.req.Body())
			require.NoError(t, err)
			early := sip.NewResponseFromRequest(tr.req, sip.StatusSessionInProgress, "Session Progress", answer)
			early.To().Params.Add("tag", "early-dialog")
			early.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
			valid := false
			var blocked *blockedEarlyMedia
			switch scenario {
			case "no body":
				early.SetBody(nil)
			case "no content type":
				early.RemoveHeader("Content-Type")
			case "wrong content type":
				early.RemoveHeader("Content-Type")
				early.AppendHeader(sip.NewHeader("Content-Type", "text/plain"))
			case "invalid SDP":
				early.SetBody([]byte("invalid"))
			case "incompatible codec":
				early.SetBody([]byte(strings.ReplaceAll(string(answer), "PCMU/8000", "unsupported/8000")))
			case "180 with SDP":
				early.StatusCode = sip.StatusRinging
			case "media failure":
				call.media = failingEarlyMedia{MediaPort: port}
			case "cancelled during setup":
				blocked = &blockedEarlyMedia{MediaPort: port, entered: make(chan struct{}), release: make(chan struct{})}
				call.media = blocked
				valid = true
			default:
				valid = true
			}
			require.NoError(t, tr.transaction.SendResponse(early))
			if blocked != nil {
				select {
				case <-blocked.entered:
				case <-time.After(time.Second):
					t.Fatal("early media setup did not start")
				}
				cancel()
				close(blocked.release)
			}
			if valid {
				require.Eventually(t, func() bool { return port.NegotiatedAudio() != nil }, time.Second, 10*time.Millisecond)
			} else if scenario != "media failure" {
				require.Never(t, func() bool { return port.NegotiatedAudio() != nil }, 40*time.Millisecond, 10*time.Millisecond)
			}
			if scenario == "cancelled" || scenario == "cancelled during setup" || scenario == "ringing timeout" || scenario == "media failure" {
				if scenario == "cancelled" {
					cancel()
				}
				select {
				case err := <-done:
					require.Error(t, err)
				case <-time.After(time.Second):
					t.Fatal("call did not abort")
				}
				require.NoError(t, tr.transaction.SendResponse(sip.NewResponseFromRequest(tr.req, sip.StatusRequestTerminated, "Terminated", nil)))
			} else {
				final := sip.NewResponseFromRequest(tr.req, sip.StatusOK, "OK", answer)
				final.To().Params.Add("tag", "early-dialog")
				switch scenario {
				case "different dialog":
					final.To().Params.Add("tag", "other-dialog")
					final.SetBody(nil)
				case "different attempt":
					final.CSeq().SeqNo++
					final.SetBody(nil)
				case "rejected":
					final.StatusCode = sip.StatusBusyHere
				}
				require.NoError(t, tr.transaction.SendResponse(final))
				if scenario != "rejected" {
					ack := h.WaitRequest(t, time.Second, req.SipCallId, "")
					require.Equal(t, sip.ACK, ack.req.Method)
				}
				switch scenario {
				case "different dialog", "different attempt":
					answerBYE(t, h, time.Second, req.SipCallId)
					require.Error(t, <-done)
				case "rejected":
					require.Error(t, <-done)
				default:
					require.NoError(t, <-done)
					closed := make(chan struct{})
					go func() { _ = call.Close(ctx); close(closed) }()
					answerBYE(t, h, time.Second, req.SipCallId)
					<-closed
				}
			}
			require.Eventually(t, port.stats.Closed.Load, time.Second, 10*time.Millisecond)
		})
	}
}

type failingEarlyMedia struct{ MediaPort }

func (failingEarlyMedia) ProcessEarlyAnswer([]byte) error { return errors.New("pipeline failure") }

type blockedEarlyMedia struct {
	MediaPort
	entered chan struct{}
	release chan struct{}
}

func (m *blockedEarlyMedia) ProcessEarlyAnswer(answer []byte) error {
	close(m.entered)
	<-m.release
	return m.MediaPort.ProcessEarlyAnswer(answer)
}

func TestMediaPortEarlyAnswerNewPeer(t *testing.T) {
	for _, encryption := range []sdp.Encryption{sdp.EncryptionNone, sdp.EncryptionRequire} {
		t.Run(fmt.Sprint(encryption), func(t *testing.T) {
			c1, c2 := newUDPPipe()
			opts := func(ip string, codecs ...string) *MediaOptions {
				return &MediaOptions{IP: newIP(ip), Encryption: encryption, Codecs: testCodecSet(codecs...)}
			}
			log := logger.NewTestLogger(t)
			m := newTestPort(t, log, c1, opts("1.1.1.1", g711.ULawSDPNameOnly, g711.ALawSDPNameOnly), RoomSampleRate)
			peer := newTestPort(t, log, c2, opts("2.2.2.2", g711.ULawSDPNameOnly), RoomSampleRate)
			offer, err := m.GenerateOffer()
			require.NoError(t, err)
			answer, err := peer.GenerateAnswer(offer)
			require.NoError(t, err)
			require.NoError(t, m.ProcessEarlyAnswer(answer))
			dst := &earlyAudioCapture{}
			m.WriteInboundAudioTo(dst)
			requireCarrierAudio(t, peer, dst)
			pipeline := m.pipeline

			c3 := newTestConn(1024)
			c3.peer.Store(c1)
			c1.peer.Store(c3)
			finalPeer := newTestPort(t, log, c3, opts("3.3.3.3", g711.ALawSDPNameOnly), RoomSampleRate)
			final, err := finalPeer.GenerateAnswer(offer)
			require.NoError(t, err)
			// A later 183 may change the media before the final answer does.
			require.NoError(t, m.ProcessEarlyAnswer(final))
			require.NotSame(t, pipeline, m.pipeline)
			require.NotNil(t, m.offer)
			requireCarrierAudio(t, finalPeer, dst)
			pipeline = m.pipeline
			require.NoError(t, m.ProcessAnswer(final))
			require.Same(t, pipeline, m.pipeline)
			requireCarrierAudio(t, finalPeer, dst)
		})
	}
}
