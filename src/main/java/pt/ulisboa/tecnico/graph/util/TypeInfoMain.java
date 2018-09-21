package pt.ulisboa.tecnico.graph.util;



import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.RemoteEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import pt.ulisboa.tecnico.graph.algorithm.pagerank.PageRankCsvOutputFormat;
import pt.ulisboa.tecnico.graph.algorithm.pagerank.PageRankParameterHelper;
import pt.ulisboa.tecnico.graph.algorithm.pagerank.SimplePageRank;
import pt.ulisboa.tecnico.graph.core.ParameterHelper;
import pt.ulisboa.tecnico.graph.output.GraphOutputFormat;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


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
