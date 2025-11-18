package com.P2PBRS.server;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.P2PBRS.peer.PeerNode;
import com.P2PBRS.server.RegistryManager.Result;

public class HeartbeatHandler extends Thread{
	
	//private final long MAX_TIME = 30;
	
	//Testing
	private final long MAX_TIME = 10;
	//Testing
	
	private final RegistryManager registry = RegistryManager.getInstance();
	
	public void run() {
		
		//System.out.println("HeartbeatHandler started"); OPTIONAL
		
		while(true) {
			
			List<PeerNode> list = registry.listPeers();
			
			for(int i=0;i<list.size();i++) {
				PeerNode p = list.get(i);	
				
				Instant now = Instant.now();
				Instant lastTimestamp = p.getLastTimestamp();
				if(lastTimestamp == null) {
					p.setLastTimestamp(now);
					lastTimestamp = p.getLastTimestamp();
				}
				
				Duration diff = Duration.between(lastTimestamp, now);
				long timeSinceLastTimestamp = diff.getSeconds();
				
				if(timeSinceLastTimestamp > MAX_TIME) {
					System.out.println("Down client " + p.getName() + " DEREGISTERING");
					Result result = registry.deregisterPeer(p.getName());
					if(!result.ok) {
						System.err.println("Failed to deregister client " + p.getName());
					}
					//Create plan for recovery if possible
				}
			}
			
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
		
	}

}
