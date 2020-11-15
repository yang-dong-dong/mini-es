package com.dd.opensource.minies;

import java.io.IOException;
import java.util.List;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.github.twohou.sonic.ChannelFactory;
import com.github.twohou.sonic.ControlChannel;
import com.github.twohou.sonic.IngestChannel;
import com.github.twohou.sonic.SearchChannel;

@SpringBootApplication
@RestController
public class MiniEsApplication {

	public static void main(String[] args) {
		SpringApplication.run(MiniEsApplication.class, args);
	}

	private String address = "127.0.0.1";
	private Integer port = 1491;
	private String password = "root";
	private Integer connectionTimeout = 5000;
	private Integer readTimeout = 5000;
	private String collection = "sample";
	private String bucket = "demo";

	private ChannelFactory factory = new ChannelFactory(address, port, password, connectionTimeout, readTimeout);

	@GetMapping("/create-index")
	public void createIndex() {

		try {

			IngestChannel ingest = factory.newIngestChannel();
			ControlChannel control = factory.newControlChannel();

			// index
			ingest.ping();
			ingest.push(collection, bucket, "1", "鹅鹅鹅，曲项向天歌。白毛浮绿水，红掌拨清波。");
			ingest.push(collection, bucket, "2",
					"花间一壶酒，独酌无相亲。  举杯邀明月，对影成三人。  月既不解饮，影徒随我身。  暂伴月将影，行乐须及春。  我歌月徘徊，我舞影零乱。  醒时同交欢，醉后各分散。  永结无情游，相期邈云汉。");
			ingest.push(collection, bucket, "3", "燕草如碧丝，秦桑低绿枝。  当君怀归日，是妾断肠时。  春风不相识，何事入罗帏？ ");
			ingest.push(collection, bucket, "4", "岱宗夫如何，齐鲁青未了。  造化钟神秀，阴阳割昏晓。  荡胸生层云，决眦入归鸟。  会当凌绝顶，一览众山小。");
			// save to disk
			control.consolidate();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@GetMapping("/search-index")
	public List<String> searchIndex(String keywords) {

		try {
			SearchChannel search = factory.newSearchChannel();
			IngestChannel ingest = factory.newIngestChannel();
			Integer resp = ingest.count(collection);
			System.out.format("Count collection: %d\n", resp);
			resp = ingest.count(collection, bucket);
			System.out.format("Count bucket: %d\n", resp);
			resp = ingest.count(collection, bucket, "1");
			System.out.format("Count object: %d\n", resp);
			// search
			search.ping();

			return search.query(collection, bucket, keywords);
//			return search.suggest(collection, bucket, keywords);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;

	}

}
