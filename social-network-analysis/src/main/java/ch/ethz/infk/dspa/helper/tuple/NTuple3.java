package ch.ethz.infk.dspa.helper.tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Extension of tuple that allows to give elements a name
 */
public class NTuple3<T0, T1, T2> extends Tuple3<T0, T1, T2> {

	private static final long serialVersionUID = 1L;

	private Map<String, Integer> nameMap = new HashMap<>();

	public NTuple3() {
	}

	public NTuple3(String name0, T0 value0, String name1, T1 value1, String name2, T2 value2) {
		super(value0, value1, value2);

		this.nameMap.put(name0, 0);
		this.nameMap.put(name1, 1);
		this.nameMap.put(name2, 2);
	}

	public static <T0, T1, T2> NTuple3<T0, T1, T2> of(String name0, T0 value0, String name1, T1 value1, String name2,
			T2 value2) {
		return new NTuple3<>(name0, value0, name1, value1, name2, value2);
	}

	public void setName(String name, int pos) {
		this.nameMap.put(name, pos);
	}

	public void setNames(List<String> names) {
		NTuple.setNames(names, getArity(), this.nameMap);
	}

	public <T> T get(String name) {
		NTuple.ensureContainsName(this.nameMap, name);
		return getField(this.nameMap.get(name));
	}

	public Map<String, Integer> getNameMap() {
		return this.nameMap;
	}

}
