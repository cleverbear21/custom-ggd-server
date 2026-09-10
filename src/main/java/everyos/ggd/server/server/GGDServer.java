package everyos.ggd.server.server;
import everyos.ggd.server.server.r1025.R1025Matchmaker;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;

import javax.net.ssl.SSLContext;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import everyos.ggd.server.common.TickTimer;
import everyos.ggd.server.common.imp.TickTimerImp;
import everyos.ggd.server.event.Event;
import everyos.ggd.server.game.Match;
import everyos.ggd.server.game.vanilla.VanillaMatch;
import everyos.ggd.server.matchmaker.MatchMaker;
import everyos.ggd.server.player.HumanPlayer;
import everyos.ggd.server.server.event.EventDecoder;
import everyos.ggd.server.server.event.EventEncoder;
import everyos.ggd.server.server.event.imp.EventDecoderImp;
import everyos.ggd.server.server.event.imp.EventEncoderImp;
import everyos.ggd.server.server.message.MessageEncoder;
import everyos.ggd.server.server.message.imp.MessageDecoderImp;
import everyos.ggd.server.server.message.imp.MessageEncoderImp;
import everyos.ggd.server.server.state.MatchMakerSocketState;
import everyos.ggd.server.server.state.MatchSocketState;
import everyos.ggd.server.server.state.SocketState;
import everyos.ggd.server.server.tls.TLSChannelWebSocketServerFactory;
import everyos.ggd.server.session.SessionData;
import everyos.ggd.server.session.SessionManager;
import everyos.ggd.server.socket.SocketArray;
import everyos.ggd.server.socket.decoder.SocketDecoder;
import everyos.ggd.server.socket.decoder.imp.SocketDecoderImp;
import everyos.ggd.server.socket.encoder.SocketEncoder;
import everyos.ggd.server.socket.encoder.imp.SocketEncoderImp;

public class GGDServer extends WebSocketServer {
	
	public static final int FRAME_RATE = 60;
	
	//TODO: Error handling
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Map<WebSocket, Client> clients = new HashMap<>();
	private final TickTimer tickTimer = new TickTimerImp(FRAME_RATE);
	private final SessionManager sessionManager = new SessionManager();
	private final MatchMaker matchMaker = new MatchMaker(sessionManager,
		id -> new VanillaMatch(id, tickTimer, () -> sessionManager.unregisterMatch(id)));
	
	private final SocketDecoder decoder = new SocketDecoderImp();
	private final SocketEncoder encoder = new SocketEncoderImp();
	private final EventEncoder eventEncoder = createEventEncoder();
	private final EventDecoder eventDecoder = new EventDecoderImp(new MessageDecoderImp());
	
	public GGDServer(int port, Optional<SSLContext> sslContext) throws UnknownHostException {
	    super(new InetSocketAddress(port));
	    sslContext.ifPresent(context -> setWebSocketFactory(new TLSChannelWebSocketServerFactory(context)));
	}

	@Override
	public void onStart() {
		logger.info("Websocket has started! (Port: " + getPort() + ")");
	}

	@Override
	public void onOpen(WebSocket conn, ClientHandshake handshake) {
		logger.info("Client connnected! (Address: " + conn.getRemoteSocketAddress() + ")");
		clients.put(conn, new Client(event -> conn.send(encodeEvent(event)), tickTimer));
	}

	@Override
	public void onClose(WebSocket conn, int code, String reason, boolean remote) {
		String reasonMsg = reason.isEmpty() ? "" : "#" + reason;
		logger.info("Client disconnnected! (Address: " + conn.getRemoteSocketAddress() + ", reason: " + code + reasonMsg + ")");
		clients.remove(conn).stop();
	}
	
	@Override
	public void onMessage(WebSocket conn, String message) {
		Client client = clients.get(conn);
		if (message.contains("matchmaker")) {
			client.setState(new MatchMakerSocketState(matchMaker));
		} else {
			logger.info("Connection to existing match requested (Address: " + conn.getRemoteSocketAddress() + ")");
			client.setState(createMatchState(message));
		}
		
		client.start();
	}

	@Override
    public void onMessage(WebSocket conn, ByteBuffer packetBuffer) {
       byte[] packet = getPacket(packetBuffer);

       try {
         if (looksLikeR1025(packet)) {
            handleR1025(conn, packet);
            return;
           }
       } catch (Exception e) {
           logger.warn("r1025 decode failed", e);
       }

       Event event = decodeEvent(ByteBuffer.wrap(packet));

       if (event.code() == Event.PING) {
        conn.send(encodeEvent(Event.createPongEvent()));
       }

       clients.get(conn).onEvent(event);
    }
	private boolean looksLikeR1025(byte[] packet) {
      if (packet.length == 0) {
        return false;
      }

      int first = packet[0] & 0xff;

      return first == 0x08 ||
           first == 0x10 ||
           first == 0x18 ||
           first == 0x20 ||
           first == 0x0a;
    }
	private void handleR1025(WebSocket conn, byte[] packet) {
      R1025Proto.Reader reader = new R1025Proto.Reader(packet);

      int control = 0;
      byte[] envelope = null;

      while (reader.hasNext()) {
          long tag = reader.readVarint();

          int field = (int) (tag >>> 3);
          int wire = (int) (tag & 7);

          switch (field) {
              case 1:
                  if (wire == 0) {
                    control = (int) reader.readVarint();
                  } else {
                    reader.skip(wire);
                  }
                  break;

              case 2:
                  if (wire == 2) {
                    envelope = reader.readBytes();
                  } else {
                    reader.skip(wire);
                  }
                  break;

              default:
                  reader.skip(wire);
                  break;
          }
      }

      logger.info(
          "r1025 packet: control={}, envelope={}",
          control,
          envelope == null ? 0 : envelope.length
      );

    // Client transport ping.
      if (control == 0) {
          conn.send(
              ByteBuffer.wrap(
                  new R1025Proto.Writer()
                      .varint(1, 1)
                      .toByteArray()
              )
          );
          return;
      }

    // Matchmaker handshake.
      if (control == 2) {
          String clientId = "ggd-" + conn.hashCode();
          String fd = "r1025-" + conn.hashCode();

          R1025Proto.Writer response =
              new R1025Proto.Writer();

          response.varint(1, 2);
          response.string(3, clientId);
          response.string(4, fd);

          conn.send(
              ByteBuffer.wrap(response.toByteArray())
          );

          logger.info(
              "Sending r1025 handshake: clientId={}, fd={}",
              clientId,
              fd
          );

          return;
      }

    // Application message.
      if (control == 3 && envelope != null) {
          R1025Proto.Reader env =
              new R1025Proto.Reader(envelope);

          String type = null;
          byte[] payload = null;

          while (env.hasNext()) {
              long tag = env.readVarint();

              int field = (int) (tag >>> 3);
              int wire = (int) (tag & 7);

              switch (field) {
                  case 1:
                      if (wire == 2) {
                        type = env.readString();
                      } else {
                        env.skip(wire);
                      }
                      break;

                  case 2:
                      if (wire == 2) {
                        payload = env.readBytes();
                      } else {
                        env.skip(wire);
                      }
                      break;

                  default:
                      env.skip(wire);
                      break;
              }
          }

          logger.info(
              "r1025 application type={}, payload={}",
              type,
              payload == null ? 0 : payload.length
          );

          if ("/m".equals(type)
                  && payload != null
                  && R1025Matchmaker.isHalloweenRequest(payload)) {

              byte[] response =
                  R1025Matchmaker.createSuccessResponse();

              conn.send(
                  R1025Matchmaker.wrapApplication(response)
              );

              logger.info(
                  "Sent r1025 HALLOWEEN matchmaker response"
              );

              return;
          }
      }
  }
	@Override
	public void onError(WebSocket conn, Exception ex) {
		
	}
	
	private SocketState createMatchState(String message) {
		SessionData session = SessionData.fromString(message);
		Match match = sessionManager.getMatch(session.matchId());
		HumanPlayer player = (HumanPlayer) match.getPlayer(session.playerId());
		if (!session.authenticationKey().equals(player.getAuthenticationKey())) {
			throw new RuntimeException("Player authentication failed");
		}
		
		return new MatchSocketState(match, player);
	}
	
	private EventEncoder createEventEncoder() {
		MessageEncoder messageEncoder = new MessageEncoderImp(encoder, decoder);
		
		return new EventEncoderImp(encoder, decoder, messageEncoder);
	}

	private Event decodeEvent(ByteBuffer packetBuffer) {
		byte[] packet = getPacket(packetBuffer);
		logger.trace(HexFormat.of().formatHex(packet));
		SocketArray packetData = decoder.decodeArray(packet, 0, packet.length);
		
		return eventDecoder.decodeEvent(packetData);
	}
	
	private byte[] encodeEvent(Event event) {
		SocketArray packetData = eventEncoder.encodeEvent(event);
		
		return encoder.encodeArray(packetData);
	}
	
	private byte[] getPacket(ByteBuffer packetBuffer) {
		byte[] packet = new byte[packetBuffer.remaining()];
		packetBuffer.get(packet);
		
		return packet;
	}
	
}
