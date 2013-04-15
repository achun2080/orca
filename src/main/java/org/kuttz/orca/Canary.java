package org.kuttz.orca;

public class Canary {

	public static void main(String[] args) {
		for (int i = 0 ; i < 100; i++) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
			}
			System.out.println("Canary sysout [" + i + "]");
			System.err.println("Canary syserr [" + i + "]");			
		}

	}
}
