package project1;

import java.io.*;
import java.util.*;


 
public class Table implements Serializable{
	
	public String TableName;
	public String ClusteringKey;
	public Vector<Page> pages;
	public int numOfPages; 
	public int pageID;
	
	public Table(String TableName, String ClusteringKey) {
		this.TableName = TableName;
		this.ClusteringKey = ClusteringKey;
		this.pages = new Vector<Page>();
		this.numOfPages=0;
		this.pageID=0;
	}
	
	
	public void increasePage() {
		numOfPages++;
	}
	
	public void decreasePage() {
		numOfPages--;
	}
	
	public void increaseID() {
		pageID++;
	}
	

}
