package ch.ethz.infk.dspa.helper.tuple;

import org.apache.flink.api.java.tuple.Tuple;

/**
 * Helper class providing the initialization functionality that Tuple provides for unnamed Tuples
 * 
 */
public abstract class NTuple {

	public static final int MAX_ARITY = 10;

	@SuppressWarnings("unchecked")
	public static Class<? extends Tuple> getTupleClass(int arity) {
		if (arity < 1 || arity > MAX_ARITY) {
			throw new IllegalArgumentException("The tuple arity must be in [1, " + MAX_ARITY + "].");
		}
		return (Class<? extends Tuple>) CLASSES[arity];
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

	private static final Class<?>[] CLASSES = new Class<?>[] {
			NTuple1.class, NTuple2.class, NTuple3.class, NTuple4.class, NTuple5.class, NTuple6.class, NTuple7.class,
			NTuple8.class, NTuple9.class, NTuple10.class
	};

}
