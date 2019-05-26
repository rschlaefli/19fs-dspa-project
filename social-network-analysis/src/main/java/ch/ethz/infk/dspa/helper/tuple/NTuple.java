package ch.ethz.infk.dspa.helper.tuple;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple;

/**
 * Helper class providing the initialization functionality that Tuple provides for unnamed Tuples
 * 
 */
public abstract class NTuple {

	public static final int MAX_ARITY = 10;
	private static final Class<?>[] CLASSES = new Class<?>[] {
			NTuple1.class, NTuple2.class, NTuple3.class, NTuple4.class, NTuple5.class, NTuple6.class, NTuple7.class,
			NTuple8.class, NTuple9.class, NTuple10.class
	};

	@SuppressWarnings("unchecked")
	public static Class<? extends Tuple> getTupleClass(int arity) {
		if (arity < 1 || arity > MAX_ARITY) {
			throw new IllegalArgumentException("The tuple arity must be in [1, " + MAX_ARITY + "].");
		}
		return (Class<? extends Tuple>) CLASSES[arity];
	}

	public static Tuple newInstance(List<String> names, List<String> values) {
		switch (names.size()) {
		case 1:
			NTuple1 tuple1 = new NTuple1<>();
			for (int i = 0; i < values.size(); i++) {
				tuple1.setName(names.get(i), i);
				tuple1.setField(values.get(i), i);
			}
			return tuple1;
		case 2:
			NTuple2 tuple2 = new NTuple2<>();
			for (int i = 0; i < values.size(); i++) {
				tuple2.setName(names.get(i), i);
				tuple2.setField(values.get(i), i);
			}
			return tuple2;
		case 3:
			NTuple3 tuple3 = new NTuple3<>();
			for (int i = 0; i < values.size(); i++) {
				tuple3.setName(names.get(i), i);
				tuple3.setField(values.get(i), i);
			}
			return tuple3;
		case 4:
			NTuple4 tuple4 = new NTuple4<>();
			for (int i = 0; i < values.size(); i++) {
				tuple4.setName(names.get(i), i);
				tuple4.setField(values.get(i), i);
			}
			return tuple4;
		case 5:
			NTuple5 tuple5 = new NTuple5<>();
			for (int i = 0; i < values.size(); i++) {
				tuple5.setName(names.get(i), i);
				tuple5.setField(values.get(i), i);
			}
			return tuple5;
		case 6:
			NTuple6 tuple6 = new NTuple6<>();
			for (int i = 0; i < values.size(); i++) {
				tuple6.setName(names.get(i), i);
				tuple6.setField(values.get(i), i);
			}
			return tuple6;
		case 7:
			NTuple7 tuple7 = new NTuple7<>();
			for (int i = 0; i < values.size(); i++) {
				tuple7.setName(names.get(i), i);
				tuple7.setField(values.get(i), i);
			}
			return tuple7;
		case 8:
			NTuple8 tuple8 = new NTuple8<>();
			for (int i = 0; i < values.size(); i++) {
				tuple8.setName(names.get(i), i);
				tuple8.setField(values.get(i), i);
			}
			return tuple8;
		case 9:
			NTuple9 tuple9 = new NTuple9<>();
			for (int i = 0; i < values.size(); i++) {
				tuple9.setName(names.get(i), i);
				tuple9.setField(values.get(i), i);
			}
			return tuple9;
		case 10:
			NTuple10 tuple10 = new NTuple10<>();
			for (int i = 0; i < values.size(); i++) {
				tuple10.setName(names.get(i), i);
				tuple10.setField(values.get(i), i);
			}
			return tuple10;
		default:
			throw new IllegalArgumentException("The tuple arity must be in [1, " + MAX_ARITY + "].");
		}
	}

	public static Tuple newInstance(int arity) {
		switch (arity) {
		case 1:
			return new NTuple1<>();
		case 2:
			return new NTuple2<>();
		case 3:
			return new NTuple3<>();
		case 4:
			return new NTuple4<>();
		case 5:
			return new NTuple5<>();
		case 6:
			return new NTuple6<>();
		case 7:
			return new NTuple7<>();
		case 8:
			return new NTuple8<>();
		case 9:
			return new NTuple9<>();
		case 10:
			return new NTuple10<>();
		default:
			throw new IllegalArgumentException("The tuple arity must be in [1, " + MAX_ARITY + "].");
		}
	}

	public static void setNames(List<String> names, int arity, Map<String, Integer> nameMap) {
		if (names.size() != arity) {
			throw new IllegalArgumentException(
					"Size of names must match arity of tuple:" + names.size() + " " + arity);
		}

		nameMap.clear();
		for (int pos = 0; pos < names.size(); pos++) {
			nameMap.put(names.get(pos), pos);
		}
	}

	public static void ensureContainsName(Map<String, Integer> nameMap, String name) {
		if (!nameMap.containsKey(name)) {
			throw new IllegalArgumentException("Access with unknown name in Named Tuple: " + name);
		}
	}
}
