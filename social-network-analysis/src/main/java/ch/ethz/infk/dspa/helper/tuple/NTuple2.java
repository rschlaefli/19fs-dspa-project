package ch.ethz.infk.dspa.helper.tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Extension of tuple that allows to give elements a name
 */
public class NTuple2<T0, T1> extends Tuple2<T0, T1> {

	private static final long serialVersionUID = 1L;

	private Map<String, Integer> nameMap = new HashMap<>();

	public NTuple2() {
	}

	public NTuple2(String name0, T0 value0, String name1, T1 value1) {
		super(value0, value1);

		this.nameMap.put(name0, 0);
		this.nameMap.put(name1, 1);
	}

	public static <T0, T1> NTuple2<T0, T1> of(String name0, T0 value0, String name1, T1 value1) {
		return new NTuple2<>(name0, value0, name1, value1);
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
