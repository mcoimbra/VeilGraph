package pt.ulisboa.tecnico.veilgraph.util;



import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;


@SuppressWarnings("unused")
public class TypeInfoMain<K extends Comparable<K>> {

	void initTypes(K k) {
		//final TypeInformation<K> keySelectorK = TypeInformation.of(new TypeHint<K>(){});


		final TypeInformation<K> keySelectorStatic = GraphUtils.getTypeInfo(k);

		final TypeInformation<Long> keySelectorLong = TypeInformation.of(new TypeHint<Long>(){});
		//TypeInformation<Edge<K, Double>> edgeTypeInfo = TypeInformation.of(new TypeHint<Edge<K, Double>>() {});
		//TypeInformation<Tuple2<K, Double>> tuple2TypeInfo = TypeInformation.of(new TypeHint<Tuple2<K, Double>>() {});


	}


	public static void main(final String[] args)
    {
		final TypeInfoMain<Long> tim = new TypeInfoMain();
		tim.initTypes(5L);


		Double ayylmao = null;

		if(ayylmao instanceof Double) {
			System.out.println("Instance of double");
		}

		if(ayylmao.getClass() == Double.class) {
			System.out.println("Class of double");
		}
	}
}
