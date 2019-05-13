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

		this.nameMap.put(name0, 0);
	}

	public static <T0> NTuple1<T0> of(String name0, T0 value0) {
		return new NTuple1<>(name0, value0);
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
