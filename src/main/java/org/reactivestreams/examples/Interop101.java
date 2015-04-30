package org.reactivestreams.examples;

import akka.actor.ActorSystem;
import akka.stream.ActorFlowMaterializer;
import akka.stream.FlowMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import org.reactivestreams.Publisher;

import ratpack.http.ResponseChunks;
import ratpack.server.RatpackServer;

import reactor.Environment;
import reactor.io.codec.StringCodec;
import reactor.io.net.NetStreams;
import reactor.io.net.http.HttpClient;
import reactor.rx.Promise;
import reactor.rx.Stream;
import reactor.rx.Streams;

import rx.Observable;
import rx.RxReactiveStreams;
import scala.runtime.BoxedUnit;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Stephane Maldini
 * Original credit and code Roland Kuhn: https://github.com/rkuhn/ReactiveStreamsInterop/blob/master/src/main/java/com/rolandkuhn/rsinterop/JavaMain.java
 */
public class Interop101 {

	static public void main(String... args) throws Exception {
		final ActorSystem system = ActorSystem.create("InteropTest");
		final FlowMaterializer mat = ActorFlowMaterializer.create(system);
		Environment.initialize().assignErrorJournal();

		final int chunks = 20;

		// RxJava Observable
		final Observable<Integer> intObs = Observable.range(1, chunks);

		// Reactive Streams Publisher
		final Publisher<Integer> intPub = RxReactiveStreams.toPublisher(intObs);

		// Akka Streams Source
		final Source<String, BoxedUnit> stringSource = Source
				.from(intPub)
				.map(Object::toString)
				.scan("", (prev, next) -> prev+next);

		// Reactive Streams Publisher
		final Publisher<String> stringPub = stringSource.runWith(Sink.fanoutPublisher(1, 1), mat);

		// Reactor Stream starting with START on the subscriber thread, then emits Akka Source with some log
		final Stream<String> linesStream = Streams
				.wrap(stringPub)
				.log("reactor.map")
				.map(i -> i + "!");


		//A Ratpack http server
		RatpackServer server = RatpackServer.of(spec -> spec
						.handlers(chain -> chain
										.get(":name", ctx ->
														// and now render the HTTP response
														ctx.render(ResponseChunks.stringChunks(ctx.stream(linesStream)))
										)
						)
		);

		server.start();


		//A Reactor http client
		HttpClient<String, String> client = NetStreams.httpClient(conf ->
						conf.connect("localhost", 5050).codec(new StringCodec())
		);

		//Fire a get request that will only ping "anchorMan"
		Promise<List<String>> result =
				client.get("/anchorMan", ch -> Streams.empty())

						//flatten the chunked results amd aggregate into a single List "Promise"
						.flatMap(replies ->
										replies
										.timeout(3, TimeUnit.SECONDS, Streams.empty())
										.log("reactor.replies")
										.toList()
						);

		//Subscribe result with RxJava ReactiveStreams bridge
		RxReactiveStreams
				.toObservable(result)
				.flatMap(Observable::from)
				.toBlocking()
				.forEach(System.out::println);

		//shutdown server
		server.stop();

		//shutdown actors
		system.shutdown();
	}
}
