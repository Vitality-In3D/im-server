package utb.fai;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.*;

public class SocketHandler {
    /** Client socket managed by this handler */
    Socket mySocket;

    /** Client identifier in format <IP_address>:<port> */
    String clientID;

    /** Reference to the shared registry of active handlers */
    ActiveHandlers activeHandlers;

    /** Message queue for pending messages to be sent to this client */
    ArrayBlockingQueue<String> messages = new ArrayBlockingQueue<String>(100);

    /** Latch to synchronize the start of InputHandler and OutputHandler */
    CountDownLatch startSignal = new CountDownLatch(2);

    /** OutputHandler handles sending data to mySocket OutputStream */
    OutputHandler outputHandler = new OutputHandler();

    /** InputHandler handles reading incoming data from mySocket InputStream */
    InputHandler inputHandler = new InputHandler();

    /** Flag indicating that input reading has finished */
    volatile boolean inputFinished = false;

    /** Identified user name for this client */
    volatile String userName = null;

    /** Set of discussion groups this client has joined */
    final Set<String> groups = ConcurrentHashMap.newKeySet();

    public SocketHandler(Socket mySocket, ActiveHandlers activeHandlers) {
        this.mySocket = mySocket;
        this.clientID = mySocket.getInetAddress().toString() + ":" + mySocket.getPort();
        this.activeHandlers = activeHandlers;
    }

    class OutputHandler implements Runnable {
        public void run() {
            OutputStreamWriter writer = null;
            try {
                startSignal.countDown();
                startSignal.await();
                writer = new OutputStreamWriter(mySocket.getOutputStream(), StandardCharsets.UTF_8);
                while (!inputFinished) {
                    // Blocking take until a message arrives
                    String m = messages.take();
                    if (inputFinished) {
                        break;
                    }
                    writer.write(m + "\r\n");
                    writer.flush();
                }
            } catch (IOException | InterruptedException e) {
                // Connection closed or thread interrupted
            } finally {
                try {
                    if (writer != null) {
                        writer.close();
                    }
                } catch (IOException ignored) {
                }
            }
        }
    }

    class InputHandler implements Runnable {
        public void run() {
            try {
                startSignal.countDown();
                startSignal.await();
                activeHandlers.add(SocketHandler.this);

                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(mySocket.getInputStream(), StandardCharsets.UTF_8));
                String line;
                while ((line = reader.readLine()) != null) {
                    String trimmed = line.trim();
                    if (trimmed.isEmpty()) {
                        continue;
                    }

                    // Before setting a nickname, only nickname setting is allowed
                    if (userName == null) {
                        if (trimmed.startsWith("#setMyName")) {
                            String name = trimmed.substring("#setMyName".length()).trim();
                            if (!name.isEmpty()) {
                                activeHandlers.registerInitialName(SocketHandler.this, name);
                            }
                        } else if (!trimmed.startsWith("#")) {
                            activeHandlers.registerInitialName(SocketHandler.this, trimmed);
                        }
                        continue;
                    }

                    // Process commands for identified clients
                    if (trimmed.startsWith("#setMyName")) {
                        String newName = trimmed.substring("#setMyName".length()).trim();
                        if (!newName.isEmpty()) {
                            activeHandlers.changeName(SocketHandler.this, newName);
                        }
                    } else if (trimmed.startsWith("#sendPrivate")) {
                        String param = trimmed.substring("#sendPrivate".length()).trim();
                        int spaceIndex = param.indexOf(' ');
                        if (spaceIndex != -1) {
                            String targetName = param.substring(0, spaceIndex).trim();
                            String message = param.substring(spaceIndex + 1);
                            activeHandlers.sendPrivate(SocketHandler.this, targetName, message);
                        }
                    } else if (trimmed.startsWith("#join")) {
                        String groupName = trimmed.substring("#join".length()).trim();
                        if (!groupName.isEmpty()) {
                            groups.add(groupName);
                            activeHandlers.addServerGroup(groupName);
                        }
                    } else if (trimmed.startsWith("#leave")) {
                        String groupName = trimmed.substring("#leave".length()).trim();
                        if (!groupName.isEmpty()) {
                            groups.remove(groupName);
                        }
                    } else if (trimmed.equals("#groups")) {
                        String joined = String.join(",", groups);
                        messages.offer(joined);
                    } else if (trimmed.equals("#allGroups")) {
                        messages.offer(activeHandlers.formatAllGroups(SocketHandler.this));
                    } else {
                        // Regular message sent to all shared group members
                        if (!trimmed.startsWith("#")) {
                            activeHandlers.sendMessageToGroups(SocketHandler.this, line);
                        }
                    }
                }
            } catch (IOException | InterruptedException e) {
                // Client disconnected
            } finally {
                inputFinished = true;
                messages.offer(""); // Unblock outputHandler
                activeHandlers.remove(SocketHandler.this);
                try {
                    mySocket.close();
                } catch (IOException ignored) {
                }
            }
        }
    }
}
