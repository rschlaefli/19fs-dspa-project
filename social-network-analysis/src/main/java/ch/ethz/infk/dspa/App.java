package ch.ethz.infk.dspa;

import ch.ethz.infk.dspa.statistics.ActivePostsStatistics;

public class App {
	public static void main(String[] args) {
		ActivePostsStatistics consumer = new ActivePostsStatistics();
		consumer.start();
	}
}
