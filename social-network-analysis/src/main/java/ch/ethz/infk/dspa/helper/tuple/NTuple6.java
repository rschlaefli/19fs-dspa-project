package ch.ethz.infk.dspa.helper.tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Extension of tuple that allows to give elements a name
 */
public class NTuple6<T0, T1, T2, T3, T4, T5> extends Tuple6<T0, T1, T2, T3, T4, T5> {

	private static final long serialVersionUID = 1L;

	private Map<String, Integer> nameMap = new HashMap<>();

	public NTuple6() {
	}

	public NTuple6(String name0, T0 value0, String name1, T1 value1, String name2, T2 value2, String name3, T3 value3,
			String name4, T4 value4, String name5, T5 value5) {
		super(value0, value1, value2, value3, value4, value5);

		nameMap.put(name0, 0);
		nameMap.put(name1, 1);
		nameMap.put(name2, 2);
		nameMap.put(name3, 3);
		nameMap.put(name4, 4);
		nameMap.put(name5, 5);
	}

	public void setName(String name, int pos) {
		nameMap.put(name, pos);
	}

	public void setNames(List<String> names) {
		if (names.size() != getArity()) {
			throw new IllegalArgumentException("Size of names must match arity of tuple");
		}

		nameMap.clear();
		for (int pos = 0; pos < names.size(); pos++) {
			nameMap.put(names.get(pos), pos);
		}
	}

	public <T> T get(String name) {

		if (!nameMap.containsKey(name)) {
			throw new IllegalArgumentException("Access with unknown name in Named Tuple");
		}

		return getField(nameMap.get(name));
	}

	public Map<String, Integer> getNameMap() {
		return nameMap;
	}

	public static <T0, T1, T2, T3, T4, T5> NTuple6<T0, T1, T2, T3, T4, T5> of(String name0, T0 value0, String name1,
			T1 value1,
			String name2, T2 value2, String name3, T3 value3, String name4, T4 value4, String name5, T5 value5) {
		return new NTuple6<>(name0, value0, name1, value1, name2, value2, name3, value3, name4, value4, name5, value5);
	}

}
