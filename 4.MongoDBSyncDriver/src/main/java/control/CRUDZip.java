package control;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.bson.Document;

import com.google.gson.Gson;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.result.InsertOneResult;

import entity.Zip;

public class CRUDZip {
	private static final Gson GSON = new Gson();
	private MongoCollection<Document> coll;

	public CRUDZip(MongoCollection<Document> coll) {
		super();
		this.coll = coll;
	}
	
	/**
	 *1/ Liệt kê tất cả các documents
	 * @param n
	 * @return list zips
	 */
	public List<Zip> getZips(int n) {
		List<Zip> zips = new ArrayList<Zip>();
		if(n > 0) {
			MongoCursor<Document> temp = coll.find().limit(n).cursor();
			while(temp.hasNext()) {
				Document doc = temp.next();
				String json = doc.toJson();
				Zip zip = GSON.fromJson(json, Zip.class);
				zips.add(zip);
			}
		}
		
		return zips;
	}
	
	/**
	 * 2/Chèn thêm 1 document mới
	 * @param zip
	 * @return
	 * @throws Exception
	 */
	public boolean addZip(Zip zip) throws Exception{
		
		String json = GSON.toJson(zip);
		Document document = Document.parse(json);
		InsertOneResult result = coll.insertOne(document);
		
		return result.getInsertedId() != null ? true : false;
	}
	
	/**
	 * Tìm 1 zip khi biết id
	 * db.zips.find({_id:'10021'})
	 * @param id
	 * @return Zip nếu tìm thấy
	 * @return null nếu không tồn tại id
	 */
	public Zip getZip(String id) {
		Zip zip = null;
		Document doc = coll.find(Document.parse("{_id:'"+id+"'}")).first();
		if(doc != null) {
			String json = doc.toJson();
			zip = GSON.fromJson(json, Zip.class);
		}
		
		return zip;
	}
	
	/**
	 * 3/ Tìm các zip theo city (có city là PALMER)
	 * @param city
	 * @return
	 */
	public List<Zip> getZipsByCity(String city) {
		List<Zip> zips = new ArrayList<Zip>();
		
		coll.createIndex(Indexes.ascending("city"));
		MongoCursor<Document> temp = coll.find(Document.parse("{city:'"+city+"'}")).iterator();
		while(temp.hasNext()) {
			Document doc = temp.next();
			String json = doc.toJson();
			Zip zip = GSON.fromJson(json, Zip.class);
			zips.add(zip);
		}
		
		return zips;
	}
	
	/**
	 * 4/ Tìm các zip theo dân số (ex: có dân số >100000)
	 * db.zips.find({pop:{$gt:100000}})
	 * @param pop
	 * @return
	 */
	public List<Zip> getZipsByPopulation(int pop) {
		List<Zip> zips = new ArrayList<Zip>();
		if(pop > 0) {
			MongoCursor<Document> temp = coll.find(Document.parse("{pop:{$gt:"+pop+"}}")).iterator();
			while(temp.hasNext()) {
				Document doc = temp.next();
				String json = doc.toJson();
				Zip zip = GSON.fromJson(json, Zip.class);
				zips.add(zip);
			}
		}
		
		return zips;
	}
	
	/**
	 * 5/ Tìm tổng dân số của thành phố (ex: PALMER)
	 * db.zips.aggregate([{$match:{city:"PALMER"}}, {$group:{_id:"$city", total:{$sum:"$pop"}}},{$project:{total:1, _id:0}}])
	 * @param city
	 * @return
	 */
	public int getPopulationByCity(String city) {
		Document doc = coll.aggregate(Arrays.asList(
				Document.parse("{$match:{city:'"+city+"'}}"),
				Document.parse("{$group:{_id:\"$city\", total:{$sum:\"$pop\"}}}"),
				Document.parse("{$project:{total:1, _id:0}}"))).first();
		
		return doc != null ? doc.getInteger("total") : 0;
	}

	/**
	 * 6/ Tìm các thành phố có tổng dân số từ - đến (từ 10 đến 50)
	 * 	db.zips.aggregate([{$group:{_id:"$city",total:{$sum:"$pop"}}},{$match:{$and:[{total:{$gte:10}}, {total:{$lte:50}}]}}])
	 * @param from
	 * @param to
	 * @return
	 */
	public Map<String, Integer> getCityByPopultion(int from, int to) {
		Map<String, Integer> result = new TreeMap<>();
		if(to >= from) {
			MongoCursor<Document> it = coll.aggregate(Arrays.asList(
					Document.parse("{$group:{_id:\"$city\",total:{$sum:\"$pop\"}}}"),
					Document.parse("{$match:{$and:[{total:{$gte:"+ from +"}}, {total:{$lte:"+ to +"}}]}}"))).iterator();
			while(it.hasNext()) {
				Document doc = it.next();
				result.put(doc.getString("_id"), doc.getInteger("total"));
			}
		}	
		
		return result;
	}
	
	/**
	 * 7/ Tìm tất cả các city của bang (ex: MA) mà có tổng dân số trên 100000
	 * 	db.zips.aggregate([{$match:{state:"MA"}}, {$group:{_id:"$city", total:{$sum:"$pop"}}}, {$match:{total:{$gt:100000}}}])
	 * @param state
	 * @param pop
	 * @return
	 */
	public Map<String, Integer> getCityByStateAndPop(String state, int pop) {
		Map<String, Integer> result = new TreeMap<>();
		if(state.length()>0 && pop > 0) {
			MongoCursor<Document> it = coll.aggregate(Arrays.asList(
					Document.parse("{$match:{state:'"+state+"'}}"),
					Document.parse("{$group:{_id:\"$city\", total:{$sum:\"$pop\"}}}"),
					Document.parse("{$match:{total:{$gt:"+pop+"}}}"))).iterator();
			while(it.hasNext()) {
				Document doc = it.next();
				result.put(doc.getString("_id"), doc.getInteger("total"));
			}
		}	
		
		return result;
	}
	
	/**
	 * 8/ Tìm tất cả các bang (distinct)
	 *  db.zips.distinct("state")
	 * 	db.zips.aggregate([{$group:{_id:"$state"}}])
	 * @return
	 */
	public List<String> getStates() {
		List<String> states = new ArrayList<>();
		coll.distinct("state", String.class).forEach(x -> states.add(x));
		
		return states;
	}
	
	/**
	 * 9/ Tìm tất cả các bang mà có chứa ít nhất 1 zip có dân số trên pop (ex: 100000)
	 * db.zips.aggregate([{$match:{pop:{$gte:100000}}},{$group:{_id:"$state"}}])
	 * @param pop
	 * @return
	 */
	public List<String> getStatesByPop(int pop) {
		List<String> list = new ArrayList<String>();
		
		MongoCursor<Document> it = coll.aggregate(Arrays.asList(
					Document.parse("{$match:{pop:{$gte:"+ pop +"}}}"),
					Document.parse("{$group:{_id:\"$state\"}}")
				)).iterator();
		
		while(it.hasNext()) {
			Document doc = it.next();
			list.add(doc.getString("_id"));
		}
		
		return list;
	}
	
	/**
	 * 10/ Tính dân số trung bình của mỗi bang
	 * db.zips.aggregate([{$group:{_id:"$state", avgPop: {$avg:"$pop"}}}])
	 * @return
	 */
	public Map<String, Double> getAvgPopulationByState() {
		Map<String, Double> result = new TreeMap<>();
		MongoCursor<Document> it = coll.aggregate(Arrays.asList(
				Document.parse("{$group:{_id:\"$state\", avgPop: {$avg:\"$pop\"}}}"))).iterator();
		while(it.hasNext()) {
			Document doc = it.next();
			result.put(doc.getString("_id"), doc.getDouble("avgPop"));
		}	

		return result;
	}
	
	/**
	 * 11/ Bang nào đó (ex: WA) có bao nhiêu city
	 * 	db.zips.aggregate([{$match:{state:"WA"}},{$group:{_id:"$city"}},{$count:"number"}])
	 * @param state
	 * @return
	 */
	public int getNumberCityByState(String state) {
		Document doc = coll.aggregate(Arrays.asList(
				Document.parse("{$match:{state:'"+state+"'}}"),
				Document.parse("{$group:{_id:\"$city\"}}"),
				Document.parse("{$count:\"number\"}"))).first();
		
		return doc != null ? doc.getInteger("number") : 0;
	}
	
	/**
	 * 12/ Tính số city của mỗi bang
	 * 	db.zips.aggregate([{$group:{_id:{state:"$state", city:"$city"}}},{$replaceRoot:{newRoot:"$_id"}}, {$group:{_id:"$state", num:{$sum:1}}}])
	 * @return
	 */
	public Map<String, Integer> getNumberCityByState() {
		Map<String, Integer> map = new TreeMap<String, Integer>();
		
		MongoCursor<Document> it = coll.aggregate(Arrays.asList(
					Document.parse("{$group:{_id:{state:\"$state\", city:\"$city\"}}}"),
					Document.parse("{$replaceRoot:{newRoot:\"$_id\"}}"),
					Document.parse("{$group:{_id:\"$state\", number:{$sum:1}}}")
				)).cursor();
		
		while(it.hasNext()) {
			Document doc = it.next();
			map.put(doc.getString("_id"), doc.getInteger("number"));
		}
		
		return map;
	}
	
	/**
	 * 13/ Tìm tất cả các bang có tổng dân số trên 10000000
	 * db.zips.aggregate([{$group:{_id:"$state", total: {$sum:"$pop"}}},{$match:{total:{$gte:10000000}}}])
	 * @param pop
	 * @return
	 */
	public Map<String, Integer> getStatesByTotalPop(int pop) {
		Map<String, Integer> map = new TreeMap<String, Integer>();
		
		MongoCursor<Document> it = coll.aggregate(Arrays.asList(
					Document.parse("{$group:{_id:\"$state\", total: {$sum:\"$pop\"}}}"),
					Document.parse("{$match:{total:{$gte:"+pop+"}}}")
				)).cursor();
		
		while(it.hasNext()) {
			Document doc = it.next();
			map.put(doc.getString("_id"), doc.getInteger("total"));
		}
		
		return map;
	}
}

