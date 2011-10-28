package search;
import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

public class CMDSearch {
	public static void main(String args[]){
		if(args.length < 4){
			System.out.println("CMDSearch :url indexDir phoneNum clientUrl parameters");
			return;
		}
		HttpClient client = new HttpClient();
		GetMethod method = new GetMethod(args[0]);
		HttpMethodParams params = new HttpMethodParams();
		params.setParameter("indexDir", "");
		params.setParameter("phoneNum", "");
		params.setParameter("clientUrl", "");
		params.setParameter("parameters", "");
		method.setParams(params);
		try {
			client.executeMethod(method);
		} catch (HttpException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
