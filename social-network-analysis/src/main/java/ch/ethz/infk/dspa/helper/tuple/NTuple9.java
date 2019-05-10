package ch.ethz.infk.dspa.helper.tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple9;

/**
 * Extension of tuple that allows to give elements a name
 */
public class NTuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8> extends Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8> {

	private static final long serialVersionUID = 1L;

	private Map<String, Integer> nameMap = new HashMap<>();

	public NTuple9() {
	}

	public NTuple9(String name0, T0 value0, String name1, T1 value1, String name2, T2 value2, String name3, T3 value3,
			String name4, T4 value4, String name5, T5 value5, String name6, T6 value6, String name7, T7 value7,
			String name8, T8 value8) {
		super(value0, value1, value2, value3, value4, value5, value6, value7, value8);

		nameMap.put(name0, 0);
		nameMap.put(name1, 1);
		nameMap.put(name2, 2);
		nameMap.put(name3, 3);
		nameMap.put(name4, 4);
		nameMap.put(name5, 5);
		nameMap.put(name6, 6);
		nameMap.put(name7, 7);
		nameMap.put(name8, 8);
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

	public static <T0, T1, T2, T3, T4, T5, T6, T7, T8> NTuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8> of(String name0,
			T0 value0,
			String name1, T1 value1, String name2, T2 value2, String name3, T3 value3, String name4, T4 value4,
			String name5,
			T5 value5, String name6, T6 value6, String name7, T7 value7, String name8, T8 value8) {
		return new NTuple9<>(name0, value0, name1, value1, name2, value2, name3, value3, name4, value4, name5, value5,
				name6, value6, name7, value7, name8, value8);
	}

}
