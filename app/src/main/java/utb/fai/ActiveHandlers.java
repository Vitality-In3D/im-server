package utb.fai;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ActiveHandlers {

    /* Set of all active connected client handlers */
    private final Set<SocketHandler> activeHandlersSet = ConcurrentHashMap.newKeySet();

    /* Mapping from unique user name to corresponding client handler */
    private final ConcurrentHashMap<String, SocketHandler> handlersByName = new ConcurrentHashMap<>();

    /* Set of all known discussion groups on the server */
    private final Set<String> allServerGroups = ConcurrentHashMap.newKeySet();

    public ActiveHandlers() {
        // "public" is the default group present from server start
        allServerGroups.add("public");
    }

    /*
     * Adds a newly connected client handler to active handlers set.
     *
     * @param handler reference to the client handler
     * 
     * @return true if added successfully
     */
    synchronized boolean add(SocketHandler handler) {
        return activeHandlersSet.add(handler);
    }

    /*
     * Removes a client handler on disconnect.
     *
     * @param handler reference to the client handler
     * 
     * @return true if removed successfully
     */
    synchronized boolean remove(SocketHandler handler) {
        if (handler.userName != null) {
            handlersByName.remove(handler.userName, handler);
        }
        return activeHandlersSet.remove(handler);
    }

    /*
     * Registers the initial nickname for a client.
     * The name must not contain spaces and must be unique.
     * Automatically adds the client to the "public" room.
     *
     * @param handler the client handler
     * 
     * @param name desired user name
     * 
     * @return true if successfully registered, false otherwise
     */
    synchronized boolean registerInitialName(SocketHandler handler, String name) {
        if (name == null || name.isEmpty() || name.contains(" ") || name.startsWith("#")) {
            return false;
        }
        if (handlersByName.containsKey(name)) {
            return false;
        }
        handler.userName = name;
        handler.groups.add("public");
        handlersByName.put(name, handler);
        allServerGroups.add("public");
        return true;
    }

    /*
     * Changes user name. If new name is not unique or invalid, keeps the old name.
     *
     * @param handler the client handler
     * 
     * @param newName requested new user name
     * 
     * @return true if name changed, false otherwise
     */
    synchronized boolean changeName(SocketHandler handler, String newName) {
        if (newName == null || newName.isEmpty() || newName.contains(" ") || newName.startsWith("#")) {
            return false;
        }
        // Name must be unique
        if (handlersByName.containsKey(newName)) {
            return false;
        }
        if (handler.userName != null) {
            handlersByName.remove(handler.userName);
        } else {
            handler.groups.add("public");
            allServerGroups.add("public");
        }
        handler.userName = newName;
        handlersByName.put(newName, handler);
        return true;
    }

    /*
     * Sends a message to all active clients that share at least one group with
     * sender.
     * The sender never receives their own message.
     *
     * @param sender the client sending the message
     * 
     * @param message message content
     */
    void sendMessageToGroups(SocketHandler sender, String message) {
        if (sender.userName == null) {
            return;
        }
        String formattedMessage = "[" + sender.userName + "] >> " + message;
        for (SocketHandler recipient : activeHandlersSet) {
            if (recipient != sender && recipient.userName != null) {
                // Check if recipient shares at least one discussion group with sender
                if (!Collections.disjoint(sender.groups, recipient.groups)) {
                    if (!recipient.messages.offer(formattedMessage)) {
                        System.err.printf("Client %s message queue is full, dropping the message!\n",
                                recipient.clientID);
                    }
                }
            }
        }
    }

    /*
     * Sends a private message directly to the target user.
     * Only the recipient receives the message.
     *
     * @param sender the client sending the message
     * 
     * @param targetName target user's nickname
     * 
     * @param message message content
     */
    void sendPrivate(SocketHandler sender, String targetName, String message) {
        if (sender.userName == null) {
            return;
        }
        SocketHandler recipient = handlersByName.get(targetName);
        if (recipient != null && recipient != sender) {
            String formattedMessage = "[" + sender.userName + "] >> " + message;
            if (!recipient.messages.offer(formattedMessage)) {
                System.err.printf("Client %s message queue is full, dropping the message!\n", recipient.clientID);
            }
        }
    }

    /*
     * Registers a new room into the set of all server rooms.
     *
     * @param group room name
     */
    void addServerGroup(String group) {
        if (group != null && !group.isEmpty()) {
            allServerGroups.add(group);
        }
    }

    /*
     * Formats all groups on the server with [+] if the user is a member,
     * or [-] if not a member (extension command #allGroups).
     *
     * @param handler the requesting client handler
     * 
     * @return comma-separated list of rooms with membership status
     */
    String formatAllGroups(SocketHandler handler) {
        StringBuilder sb = new StringBuilder();
        for (String g : allServerGroups) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(handler.groups.contains(g) ? "[+] " : "[-] ").append(g);
        }
        return sb.toString();
    }
}
