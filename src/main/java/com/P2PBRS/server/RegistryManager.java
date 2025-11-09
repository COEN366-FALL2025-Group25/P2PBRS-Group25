package com.P2PBRS.server;

import com.P2PBRS.peer.PeerNode;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.net.InetAddress;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RegistryManager {
    private static final RegistryManager INSTANCE = new RegistryManager();
    private static final int maxPeers = 1000;

    private final Path statePath = Paths.get("data", "registry.yaml");
    private final Map<String, PeerNode> peersByName = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock rw = new ReentrantReadWriteLock();
    private final Set<String> validRoles = Set.of("OWNER", "STORAGE", "BOTH");
    private final Yaml yaml;

    private RegistryManager() {
        DumperOptions opts = new DumperOptions();
        opts.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        opts.setPrettyFlow(true);
        LoaderOptions loaderOptions = new LoaderOptions();

        DumperOptions dumperOptions = new DumperOptions();
        dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        dumperOptions.setPrettyFlow(true);

        this.yaml = new Yaml(loaderOptions, dumperOptions);
        loadFromDisk();
    }

    public static RegistryManager getInstance() {
        return INSTANCE;
    }

    public Result registerPeer(PeerNode peer) {
        rw.writeLock().lock();
        try {
            // Validate
            if (!validRoles.contains(peer.getRole())) {
                return Result.error("ERROR: Invalid role specified");
            }
            if (peersByName.size() >= maxPeers) {
                return Result.error("ERROR: Server capacity exceeded");
            }
            if (peersByName.containsKey(peer.getName())) {
                return Result.error("ERROR: Name already registered");
            }
            if (isAddressInUse(peer.getIpAddress(), peer.getUdpPort(), peer.getTcpPort())) {
                return Result.error("ERROR: IP/port combination already in use");
            }

            peersByName.put(peer.getName(), peer);
            persist();
            return Result.ok();
        } finally {
            rw.writeLock().unlock();
        }
    }

    public List<PeerNode> listPeers() {
        rw.readLock().lock();
        try {
            return new java.util.ArrayList<>(peersByName.values());
        } finally {
            rw.readLock().unlock();
        }
    }

    public Result deregisterPeer(String name) {
        rw.writeLock().lock();
        try {
            PeerNode removed = peersByName.remove(name);
            if (removed == null) {
                return Result.error("ERROR: Name not registered");
            }
            persist();
            return Result.ok();
        } finally {
            rw.writeLock().unlock();
        }
    }

    public Optional<PeerNode> getPeer(String name) {
        rw.readLock().lock();
        try {
            return Optional.ofNullable(peersByName.get(name));
        } finally {
            rw.readLock().unlock();
        }
    }

    public Optional<PeerNode> findPeerByEndpoint(InetAddress addr, int udpPort) {
        String ip = addr.getHostAddress(); // normalized textual IP
        rw.readLock().lock();
        try {
            for (PeerNode p : peersByName.values()) {
                if (ip.equals(p.getIpAddress()) && p.getUdpPort() == udpPort) {
                    return Optional.of(p);
                }
            }
            return Optional.empty();
        } finally {
            rw.readLock().unlock();
        }
    }

    public int size() {
        return peersByName.size();
    }

    private boolean isAddressInUse(String ip, int udpPort, int tcpPort) {
        for (PeerNode p : peersByName.values()) {
            if (p.getIpAddress().equals(ip) &&
                (p.getUdpPort() == udpPort || p.getTcpPort() == tcpPort)) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private void loadFromDisk() {
        rw.writeLock().lock();
        try {
            Files.createDirectories(statePath.getParent());
            if (!Files.exists(statePath)) {
                persist(); // create empty file on first run
                return;
            }
            try (Reader r = Files.newBufferedReader(statePath)) {
                Object loaded = yaml.load(r);
                Map<String, Object> root = (loaded instanceof Map) ? (Map<String, Object>) loaded : new LinkedHashMap<>();

                // Expected YAML shape:
                // peers:
                //   <name>:
                //     role: ...
                //     ipAddress: ...
                //     udpPort: ...
                //     tcpPort: ...
                //     storageCapacity: ...
                //     registeredAt: ...
                Map<String, Object> peers = (Map<String, Object>) root.getOrDefault("peers", Collections.emptyMap());
                peersByName.clear();
                for (Map.Entry<String, Object> e : peers.entrySet()) {
                    String name = e.getKey();
                    Map<String, Object> m = (Map<String, Object>) e.getValue();
                    PeerNode p = new PeerNode(
                            name,
                            asString(m.get("role")),
                            asString(m.get("ipAddress")),
                            asInt(m.get("udpPort")),
                            asInt(m.get("tcpPort")),
                            asInt(m.get("storageCapacity"))
                    );
                    p.setRegisteredAt(asString(m.get("registeredAt")));
                    peersByName.put(name, p);
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to load registry.yaml: " + e.getMessage());
        } finally {
            rw.writeLock().unlock();
        }
    }

    private void persist() {
        Map<String, Object> root = new LinkedHashMap<>();
        Map<String, Object> peersOut = new LinkedHashMap<>();

        for (PeerNode p : peersByName.values()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("role", p.getRole());
            m.put("ipAddress", p.getIpAddress());
            m.put("udpPort", p.getUdpPort());
            m.put("tcpPort", p.getTcpPort());
            m.put("storageCapacity", p.getStorageCapacity());
            m.put("registeredAt", p.getRegisteredAt());
            peersOut.put(p.getName(), m);
        }
        root.put("peers", peersOut);

        try (Writer w = Files.newBufferedWriter(statePath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            yaml.dump(root, w);
        } catch (IOException e) {
            System.err.println("Failed to persist registry.yaml: " + e.getMessage());
        }
    }

    private static String asString(Object o) { return o == null ? null : String.valueOf(o); }
    private static int asInt(Object o) { return o == null ? 0 : Integer.parseInt(String.valueOf(o)); }

    public static class Result {
        public final boolean ok;
        public final String message;
        private Result(boolean ok, String message) { this.ok = ok; this.message = message; }
        public static Result ok() { return new Result(true, "OK"); }
        public static Result error(String msg) { return new Result(false, msg); }
    }
}
