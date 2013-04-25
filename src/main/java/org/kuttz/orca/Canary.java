package org.kuttz.orca;

import java.io.File;
import java.net.URL;

public class Canary {

	public static void main(String[] args) {
		URL location = Canary.class.getProtectionDomain().getCodeSource().getLocation();
		System.out.println("Location : " + location.getFile());
		
		File file = new File(location.getPath());
		String[] list = file.getParentFile().list();
		for (String f : list) {
			System.out.println("File found : " + f);
		}
		
		File file2 = new File(".");
		String[] list2 = file2.list();
		for (String f : list2) {
			System.out.println("File found new : " + f);
		}		
		for (int i = 0 ; i < 5; i++) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
			}
			System.out.println("Canary sysout [" + i + "]");
			System.err.println("Canary syserr [" + i + "]");			
		}

	}
}
