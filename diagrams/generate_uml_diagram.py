#!/usr/bin/env python3
"""
Generate UML Architecture Diagram for P2PBRS System
Section 2.1 - Architecture Overview
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, ConnectionPatch
import matplotlib.patches as patches

def create_uml_diagram():
    """Create a UML component diagram showing P2PBRS architecture"""
    
    fig, ax = plt.subplots(1, 1, figsize=(16, 12))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)
    ax.axis('off')
    
    # Define colors
    server_color = '#E8F4F8'
    peer_color = '#FFF4E6'
    module_color = '#F0F0F0'
    text_color = '#333333'
    
    # ========== SERVER COMPONENT ==========
    # Server main box
    server_box = FancyBboxPatch((0.5, 6.5), 2.5, 3, 
                                boxstyle="round,pad=0.1", 
                                facecolor=server_color, 
                                edgecolor='#0066CC', 
                                linewidth=2)
    ax.add_patch(server_box)
    ax.text(1.75, 9.2, 'Server', ha='center', va='center', 
            fontsize=16, fontweight='bold', color='#0066CC')
    
    # Server modules
    modules = [
        ('UDPServer', 1.75, 8.5),
        ('ClientHandler', 1.75, 8.0),
        ('RegistryManager', 1.75, 7.5),
        ('HeartbeatHandler', 1.75, 7.0),
    ]
    
    for name, x, y in modules:
        module_box = FancyBboxPatch((x-0.6, y-0.15), 1.2, 0.3,
                                    boxstyle="round,pad=0.05",
                                    facecolor=module_color,
                                    edgecolor='#666666',
                                    linewidth=1)
        ax.add_patch(module_box)
        ax.text(x, y, name, ha='center', va='center', 
                fontsize=9, color=text_color)
    
    # ========== PEER COMPONENTS ==========
    peer_positions = [
        # (x, y, name, role, color)
        (4.5, 7.5, 'Peer 1\n(OWNER)', '#FFE6E6'),
        (4.5, 5.5, 'Peer 2\n(OWNER)', '#FFE6E6'),
        (7.5, 7.5, 'Storage\nPeer 1', '#E6FFE6'),
        (7.5, 5.5, 'Storage\nPeer 2', '#E6FFE6'),
    ]
    
    peer_boxes = []
    for x, y, name, color in peer_positions:
        peer_box = FancyBboxPatch((x-0.8, y-0.6), 1.6, 1.2,
                                  boxstyle="round,pad=0.1",
                                  facecolor=color,
                                  edgecolor='#006600' if 'Storage' in name else '#CC0000',
                                  linewidth=2)
        ax.add_patch(peer_box)
        ax.text(x, y, name, ha='center', va='center',
                fontsize=11, fontweight='bold',
                color='#006600' if 'Storage' in name else '#CC0000')
        peer_boxes.append((x, y))
    
    # Peer internal modules (shown for one peer as example)
    peer_modules = [
        ('UDPClient', 4.5, 6.8),
        ('TCP Server', 4.5, 6.3),
        ('HeartbeatSender', 4.5, 5.8),
        ('CLI', 4.5, 5.3),
    ]
    
    for name, x, y in peer_modules:
        module_box = FancyBboxPatch((x-0.5, y-0.12), 1.0, 0.24,
                                    boxstyle="round,pad=0.05",
                                    facecolor=module_color,
                                    edgecolor='#666666',
                                    linewidth=0.8)
        ax.add_patch(module_box)
        ax.text(x, y, name, ha='center', va='center',
                fontsize=8, color=text_color)
    
    # ========== COMMUNICATION LINES ==========
    # UDP connections (dashed, blue)
    udp_connections = [
        # From Server to Peers
        ((1.75, 8.0), (4.5, 7.5), 'UDP\nControl'),
        ((1.75, 8.0), (4.5, 5.5), 'UDP\nControl'),
        ((1.75, 8.0), (7.5, 7.5), 'UDP\nControl'),
        ((1.75, 8.0), (7.5, 5.5), 'UDP\nControl'),
    ]
    
    for (x1, y1), (x2, y2), label in udp_connections:
        arrow = FancyArrowPatch((x1, y1), (x2, y2),
                               arrowstyle='->', 
                               linestyle='--',
                               color='#0066CC',
                               linewidth=1.5,
                               alpha=0.7)
        ax.add_patch(arrow)
        # Label
        mid_x, mid_y = (x1 + x2) / 2, (y1 + y2) / 2
        ax.text(mid_x, mid_y + 0.3, label, ha='center', va='center',
                fontsize=7, color='#0066CC', 
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
    
    # TCP connections (solid, green) - between peers
    tcp_connections = [
        # Peer 1 to Storage Peers
        ((4.5, 7.0), (7.5, 7.5), 'TCP\nChunks'),
        ((4.5, 7.0), (7.5, 5.5), 'TCP\nChunks'),
        # Peer 2 to Storage Peers
        ((4.5, 5.0), (7.5, 7.5), 'TCP\nChunks'),
        ((4.5, 5.0), (7.5, 5.5), 'TCP\nChunks'),
    ]
    
    for (x1, y1), (x2, y2), label in tcp_connections:
        arrow = FancyArrowPatch((x1, y1), (x2, y2),
                               arrowstyle='->',
                               linestyle='-',
                               color='#00AA00',
                               linewidth=2,
                               alpha=0.8)
        ax.add_patch(arrow)
        # Label
        mid_x, mid_y = (x1 + x2) / 2, (y1 + y2) / 2
        ax.text(mid_x, mid_y - 0.3, label, ha='center', va='center',
                fontsize=7, color='#00AA00',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
    
    # ========== LEGEND ==========
    legend_elements = [
        mpatches.Patch(facecolor=server_color, edgecolor='#0066CC', 
                      linewidth=2, label='Server Component'),
        mpatches.Patch(facecolor='#FFE6E6', edgecolor='#CC0000', 
                      linewidth=2, label='Owner Peer'),
        mpatches.Patch(facecolor='#E6FFE6', edgecolor='#006600', 
                      linewidth=2, label='Storage Peer'),
        mpatches.Patch(facecolor='white', edgecolor='#0066CC', 
                      linestyle='--', linewidth=1.5, label='UDP (Control)'),
        mpatches.Patch(facecolor='white', edgecolor='#00AA00', 
                      linestyle='-', linewidth=2, label='TCP (Data)'),
    ]
    
    ax.legend(handles=legend_elements, loc='lower left', 
             fontsize=9, framealpha=0.9)
    
    # ========== TITLE ==========
    ax.text(5, 9.8, 'P2PBRS System Architecture', 
           ha='center', va='center',
           fontsize=20, fontweight='bold', color='#000033')
    
    ax.text(5, 9.4, 'Section 2.1 - Architecture Overview', 
           ha='center', va='center',
           fontsize=12, style='italic', color='#666666')
    
    # ========== ANNOTATIONS ==========
    # Add note about data flow
    note_text = ('Communication Flow:\n'
                '• UDP: Registration, Backup Requests, Heartbeats\n'
                '• TCP: Chunk transfers between peers\n'
                '• Server coordinates but never handles file data')
    
    ax.text(5, 3.5, note_text, ha='center', va='top',
           fontsize=9, color=text_color,
           bbox=dict(boxstyle='round,pad=0.5', facecolor='#FFFFE0', 
                    edgecolor='#CCCC00', linewidth=1.5))
    
    plt.tight_layout()
    return fig

def create_class_diagram():
    """Create a UML class diagram showing key classes and relationships"""
    
    fig, ax = plt.subplots(1, 1, figsize=(18, 14))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 10)
    ax.axis('off')
    
    # Define colors
    class_color = '#E8F4F8'
    text_color = '#333333'
    
    # ========== SERVER CLASSES ==========
    server_classes = [
        # (x, y, width, height, class_name, attributes, methods)
        (1, 7.5, 2, 1.5, 'ServerMain', 
         ['+main(args: String[])'], 
         ['+help()']),
        (1, 5.5, 2, 1.5, 'UDPServer', 
         ['-port: int', '-socket: DatagramSocket'], 
         ['+start(): void']),
        (1, 3, 2, 2, 'ClientHandler', 
         ['-packet: DatagramPacket', '-socket: DatagramSocket', 
          '-registry: RegistryManager'], 
         ['+run(): void', '-processMessage(): String', 
          '-processRegister(): String', '-processBackup(): String']),
        (4, 7, 2.5, 2, 'RegistryManager', 
         ['-peersByName: Map<String,PeerNode>', 
          '-fileChunkOwners: Map<String,Map>', 
          '-rw: ReentrantReadWriteLock'], 
         ['+registerPeer(): Result', '+deregisterPeer(): Result', 
          '+getPeer(): Optional<PeerNode>', '+persist(): void']),
        (4, 4.5, 2.5, 2, 'HeartbeatHandler', 
         ['-registry: RegistryManager', '-recoveringPeers: Set<String>'], 
         ['+run(): void', '-triggerRecovery(): void', 
          '-selectNewStoragePeer(): String']),
    ]
    
    for x, y, w, h, name, attrs, methods in server_classes:
        # Class box
        class_box = FancyBboxPatch((x, y), w, h,
                                   boxstyle="round,pad=0.1",
                                   facecolor=class_color,
                                   edgecolor='#0066CC',
                                   linewidth=2)
        ax.add_patch(class_box)
        
        # Class name
        ax.text(x + w/2, y + h - 0.2, name, ha='center', va='top',
               fontsize=11, fontweight='bold', color='#0066CC')
        
        # Separator line
        ax.plot([x + 0.1, x + w - 0.1], [y + h - 0.5, y + h - 0.5],
               color='#0066CC', linewidth=1)
        
        # Attributes
        attr_y = y + h - 0.7
        for attr in attrs:
            ax.text(x + 0.15, attr_y, attr, ha='left', va='top',
                   fontsize=8, color=text_color, family='monospace')
            attr_y -= 0.25
        
        # Separator line
        if methods:
            ax.plot([x + 0.1, x + w - 0.1], [attr_y, attr_y],
                   color='#0066CC', linewidth=1)
        
        # Methods
        method_y = attr_y - 0.1
        for method in methods:
            ax.text(x + 0.15, method_y, method, ha='left', va='top',
                   fontsize=8, color=text_color, family='monospace')
            method_y -= 0.25
    
    # ========== PEER CLASSES ==========
    peer_classes = [
        (7.5, 7.5, 2.5, 2, 'PeerMain', 
         ['-storagePeerIps: Map<String,String>', 
          '-storagePeerPorts: Map<String,Integer>', 
          '-fileChecksums: Map<String,Long>'], 
         ['+main(): void', '+restoreFileChunks(): void', 
          '+nextRequest(): int']),
        (7.5, 5, 2.5, 1.5, 'PeerNode', 
         ['-name: String', '-role: String', '-ipAddress: String', 
          '-udpPort: int', '-tcpPort: int', '-storageCapacity: int'], 
         ['+getName(): String', '+getRole(): String']),
        (7.5, 3, 2.5, 1.5, 'UDPClient', 
         ['-serverHost: String', '-serverPort: int', 
          '-socket: DatagramSocket', '-pending: Map<Integer,PendingRequest>'], 
         ['+sendRegister(): String', '+sendBackupReq(): String', 
          '+sendHeartbeat(): String']),
        (10.5, 7, 2, 1.5, 'HeartbeatSender', 
         ['-client: UDPClient', '-self: PeerNode', '-running: boolean'], 
         ['+run(): void', '+stopHeartbeat(): void']),
    ]
    
    for x, y, w, h, name, attrs, methods in peer_classes:
        # Class box
        class_box = FancyBboxPatch((x, y), w, h,
                                   boxstyle="round,pad=0.1",
                                   facecolor='#FFF4E6',
                                   edgecolor='#CC6600',
                                   linewidth=2)
        ax.add_patch(class_box)
        
        # Class name
        ax.text(x + w/2, y + h - 0.2, name, ha='center', va='top',
               fontsize=11, fontweight='bold', color='#CC6600')
        
        # Separator line
        ax.plot([x + 0.1, x + w - 0.1], [y + h - 0.5, y + h - 0.5],
               color='#CC6600', linewidth=1)
        
        # Attributes
        attr_y = y + h - 0.7
        for attr in attrs[:3]:  # Limit to 3 for space
            ax.text(x + 0.15, attr_y, attr, ha='left', va='top',
                   fontsize=8, color=text_color, family='monospace')
            attr_y -= 0.25
        
        # Methods
        if methods:
            ax.plot([x + 0.1, x + w - 0.1], [attr_y, attr_y],
                   color='#CC6600', linewidth=1)
            method_y = attr_y - 0.1
            for method in methods[:2]:  # Limit to 2 for space
                ax.text(x + 0.15, method_y, method, ha='left', va='top',
                       fontsize=8, color=text_color, family='monospace')
                method_y -= 0.25
    
    # ========== RELATIONSHIPS ==========
    # ServerMain -> UDPServer
    arrow1 = FancyArrowPatch((1.5, 7.5), (1.5, 7),
                            arrowstyle='->', color='#666666', linewidth=1.5)
    ax.add_patch(arrow1)
    ax.text(1.5, 7.25, 'creates', ha='center', va='center',
           fontsize=7, color='#666666')
    
    # UDPServer -> ClientHandler
    arrow2 = FancyArrowPatch((2, 6.5), (1, 4),
                            arrowstyle='->', color='#666666', linewidth=1.5)
    ax.add_patch(arrow2)
    ax.text(1.5, 5.2, 'spawns', ha='center', va='center',
           fontsize=7, color='#666666')
    
    # ClientHandler -> RegistryManager
    arrow3 = FancyArrowPatch((1.5, 3.5), (4, 6.5),
                            arrowstyle='->', color='#666666', linewidth=1.5)
    ax.add_patch(arrow3)
    ax.text(2.75, 5, 'uses', ha='center', va='center',
           fontsize=7, color='#666666')
    
    # PeerMain -> PeerNode
    arrow4 = FancyArrowPatch((8.5, 5), (8.5, 5.5),
                            arrowstyle='->', color='#666666', linewidth=1.5)
    ax.add_patch(arrow4)
    ax.text(8.5, 5.25, 'has', ha='center', va='center',
           fontsize=7, color='#666666')
    
    # PeerMain -> UDPClient
    arrow5 = FancyArrowPatch((7.5, 5.5), (7.5, 4.5),
                            arrowstyle='->', color='#666666', linewidth=1.5)
    ax.add_patch(arrow5)
    ax.text(7.5, 5, 'uses', ha='center', va='center',
           fontsize=7, color='#666666')
    
    # PeerMain -> HeartbeatSender
    arrow6 = FancyArrowPatch((9, 7.5), (10.5, 7.75),
                            arrowstyle='->', color='#666666', linewidth=1.5)
    ax.add_patch(arrow6)
    ax.text(9.75, 7.6, 'creates', ha='center', va='center',
           fontsize=7, color='#666666')
    
    # Title
    ax.text(6, 9.5, 'P2PBRS Class Diagram', 
           ha='center', va='center',
           fontsize=18, fontweight='bold', color='#000033')
    
    plt.tight_layout()
    return fig

if __name__ == '__main__':
    print("Generating UML Architecture Diagram...")
    fig1 = create_uml_diagram()
    fig1.savefig('architecture_diagram.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: architecture_diagram.png")
    
    print("Generating UML Class Diagram...")
    fig2 = create_class_diagram()
    fig2.savefig('class_diagram.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: class_diagram.png")
    
    print("\nDiagrams generated successfully!")
    print("Files created:")
    print("  - architecture_diagram.png (Section 2.1)")
    print("  - class_diagram.png (Additional class diagram)")

