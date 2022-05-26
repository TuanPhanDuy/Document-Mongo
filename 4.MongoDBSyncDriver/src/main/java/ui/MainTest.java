package ui;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import control.CRUDZip;
import entity.Zip;

public class MainTest {
	public static void main(String[] args) {
		MongoClient client = MongoClients.create();//127.0.0.1, 27017
		MongoDatabase db = client.getDatabase("zipdb");
		MongoCollection<Document> coll = db.getCollection("zips");
		CRUDZip crudZip = new CRUDZip(coll);
		
//		1/ Liệt kê tất cả các documents
		System.out.println("1/ Liệt kê tất cả các documents");
		crudZip.getZips(5).forEach(z -> System.out.println(z));
		
//		2/Chèn thêm 1 document mới
		System.out.println("\n2/ Chèn thêm 1 document mới");
		try {
			Zip zip = new Zip("123456789", "AMAHA", new double[] {3.4543, 530.45}, 1000, "WA");
			boolean x = crudZip.addZip(zip);
			if(x)
				System.out.println("Inserted!");
		}catch (Exception e) {
			System.out.println(e.getMessage());
		}
		
//		Tìm 1 zip khi biết id
		System.out.println("\nTìm 1 zip khi biết id");
		String id = "1234567890";
		Zip zip = crudZip.getZip(id);
		if(zip != null)
			System.out.println(zip);
		else
			System.out.println("Không tồn tại Zip có id = " + id);
		
//		3/ Tìm các zip theo city (có city là PALMER)
		System.out.println("\n3/ Tìm các zip theo city (ex: có city là PALMER)");
		crudZip.getZipsByCity("CHESTER").forEach(x ->System.out.println(x));
		
//		4/ Tìm các zip theo dân số (ex: có dân số >100000)
		System.out.println("\n4/ Tìm các zip theo dân số (ex: có dân số >100000)");
		crudZip.getZipsByPopulation(100000).forEach(x ->System.out.println(x));
		
		
//		5/ Tìm tổng dân số của thành phố (ex: PALMER)
		System.out.println("\n5/ Tìm tổng dân số của thành phố (ex: PALMER)");
		int totalPop = crudZip.getPopulationByCity("PALMER_111111111");
		System.out.println("Tổng dân số: " + totalPop);
		
//		6/ Tìm các thành phố có tổng dân số từ - đến (ex: từ 10 đến 50)
		System.out.println("\n6/ Tìm các thành phố có tổng dân số từ - đến (từ 10 đến 20)");
		crudZip.getCityByPopultion(10, 20).forEach((city, total)-> {
			System.out.println("City: " + city);
			System.out.println("Total population: " + total);
			System.out.println("=====================");
		});
		
//		7/ Tìm tất cả các city của bang (ex: MA) mà có tổng dân số trên pop (ex: 100000)
		System.out.println("\n7/ Tìm tất cả các city của bang (ex: MA) mà có tổng dân số trên pop (ex: 100000)");
		crudZip.getCityByStateAndPop("MA", 100000).forEach((city, pop) -> {
			System.out.println("City: " + city);
			System.out.println("Total population: " + pop);
			System.out.println("==================");
		});
		
//		 8/ Tìm tất cả các bang (distinct)
		System.out.println("\n8/ Tìm tất cả các bang (distinct)");
		crudZip.getStates().forEach(st -> System.out.println("State: " + st));
		
//		9/ Tìm tất cả các bang mà có chứa ít nhất 1 zip có dân số trên pop (ex: 100000)
		System.out.println("\n9/ Tìm tất cả các bang mà có chứa ít nhất 1 zip có dân số trên pop (ex: 100000)");
		crudZip.getStatesByPop(100000).forEach(st -> System.out.println("State: " + st));
		
//		10/ Tính dân số trung bình của mỗi bang
		System.out.println("\n10/ Tính dân số trung bình của mỗi bang");
		crudZip.getAvgPopulationByState().forEach((state, avgPop) -> {
			System.out.println("State: " +state);
			System.out.println("Avg population: " + avgPop);
			System.out.println("====================");
		});
		
//		11/ Bang nào đó (ex: WA) có bao nhiêu city
		System.out.println("\n11/ Bang nào đó (ex: WA) có bao nhiêu city");
		int number = crudZip.getNumberCityByState("WA");
		System.out.println("Number of city: " + number);
		
//		12/ Tính số city của mỗi bang
		System.out.println("\n12/ Tính số city của mỗi bang");
		crudZip.getNumberCityByState().forEach((state,num) -> {
			System.out.println("State: " + state);
			System.out.println("Number of city: " + num);
			System.out.println("========================");
		});
		
//		13/ Tìm tất cả các bang có tổng dân số trên 10000000
		System.out.println("\n13/ Tìm tất cả các bang có tổng dân số trên 10000000");
		crudZip.getStatesByTotalPop(10000000).forEach((state, total) -> {
			System.out.println("State: " +state);
			System.out.println("Total population: " + total);
			System.out.println("====================");
		});
		
	}
}
