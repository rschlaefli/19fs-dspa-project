package ch.ethz.infk.dspa.helper.tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Extension of tuple that allows to give elements a name
 */
public class NTuple1<T0> extends Tuple1<T0> {

	private static final long serialVersionUID = 1L;

	private Map<String, Integer> nameMap = new HashMap<>();

	public NTuple1() {
	}

	public NTuple1(String name0, T0 value0) {
		super(value0);

		nameMap.put(name0, 0);
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

	public static <T0> NTuple1<T0> of(String name0, T0 value0) {
		return new NTuple1<>(name0, value0);
	}

}
