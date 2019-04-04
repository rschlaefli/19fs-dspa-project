package ch.ethz.infk.dspa.recommendations.dto;

import java.util.HashMap;

public class PersonActivity {
	public Long personId;
	public Long postId;

	// TODO maybe use other key than string
	public HashMap<String, Integer> categoryMap;

}
