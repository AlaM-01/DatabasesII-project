package project1;

import java.util.*;

import java.io.*;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DBApp {

	public DBApp() throws IOException {
		this.init();
	}

	public void init() throws IOException {

		File tables = new File("Tables");
		tables.mkdirs();

		FileWriter writer = new FileWriter("metadata.csv");
		String[] columnNames = { "Table Name", "Column Name", "Column Type", "Clustering Key", "Index Name",
				"Index Type", "Min", "Max" };

		// Write the column names as the first row
		for (String columnName : columnNames) {
			writer.append(columnName);
			writer.append(",");
		}
		writer.append("\n");

		writer.flush();
		writer.close();
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws DBAppException, IOException {

		// input validation
		for (String col : htblColNameType.values()) {
			if (!col.equals("java.lang.Integer") && !col.equals("java.lang.String") && !col.equals("java.lang.double")
					&& !col.equals("java.util.Date"))
				throw new DBAppException();
		}

		for (String col : htblColNameType.keySet()) {
			if (!htblColNameMin.containsKey(col) || !htblColNameMax.containsKey(col))
				throw new DBAppException();
		}

		for (String col : htblColNameMin.keySet()) {
			if (!htblColNameType.containsKey(col) || !htblColNameMax.containsKey(col))
				throw new DBAppException();
		}

		for (String col : htblColNameMax.keySet()) {
			if (!htblColNameType.containsKey(col) || !htblColNameMin.containsKey(col))
				throw new DBAppException();
		}

		// reads from the metadata csv to check if the table already exists or not
		Vector<String> values = new Vector<>();
		String line;
		String[] headers = null;
		int columnIdx = -1;
		FileReader fr = new FileReader("metadata.csv");
		BufferedReader br = new BufferedReader(fr);

		while ((line = br.readLine()) != null) {
			String[] row = line.split(",");

			if (headers == null) {
				headers = row;

				for (int i = 0; i < headers.length; i++) {
					if (headers[i].equals("Table Name")) {
						columnIdx = i;
						break;
					}
				}
			} else {
				values.add(row[columnIdx]);
			}
		}

		if (values.contains(strTableName))
			throw new DBAppException();

		// creating the table instance and adding it to the table folder, serializing it
		// too
		Table newTable = new Table(strTableName, strClusteringKeyColumn);
		String path = "Tables/" + strTableName + ".ser";

		serializeObject(newTable, path);

		// adding to the metadata
		FileWriter writer = new FileWriter("metadata.csv", true);

		for (String key : htblColNameType.keySet()) {

			writer.append(strTableName);
			writer.append(",");
			writer.append(key);
			writer.append(",");
			writer.append(htblColNameType.get(key));
			writer.append(",");
			writer.append(key.toLowerCase().equals(strClusteringKeyColumn.toLowerCase()) + "");
			writer.append(",");
			writer.append("null");
			writer.append(",");
			writer.append("null");
			writer.append(",");
			writer.append(htblColNameMin.get(key));
			writer.append(",");
			writer.append(htblColNameMax.get(key));
			writer.append("\n");

		}

		writer.flush();
		writer.close();

		File forPages = new File(strTableName);
		forPages.mkdirs();

	}

	// this method is for validating that the record matches the table's data types
	// and number of
	// attributes
	public static boolean validate(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ParseException {
		int validCounter = 0;
		Set<String> columnNames = htblColNameValue.keySet();
		BufferedReader reader = null;
		String line = "";

		reader = new BufferedReader(new FileReader("metadata.csv"));
		while ((line = reader.readLine()) != null) {
			if (line.startsWith(strTableName)) {
				String[] row = line.split(",");

				String currentType = "class " + row[2];

				for (String cname : columnNames) {
					String colType = htblColNameValue.get(cname).getClass() + "";

					if (cname.equals(row[1]) && colType.equals(currentType)) {
						Object currentValue = htblColNameValue.get(cname);
						String min = row[6];
						String max = row[7];

						validCounter += 1;

						if (colType.equals("class java.lang.Integer")) {
							int currentInt = Integer.valueOf("" + currentValue);
							int minInt = Integer.valueOf(min);
							int maxInt = Integer.valueOf(max);
							if (currentInt < minInt || currentInt > maxInt) {
								validCounter -= 1;
							}

						}

						if (colType.equals("class java.lang.String")) {
							String currentString = currentValue + "";

							if (currentString.compareToIgnoreCase(min) < 0
									|| currentString.compareToIgnoreCase(max) > 0) {
								validCounter -= 1;

							}

						}

						if (colType.equals("class java.lang.Double")) {
							Double currentDouble = Double.valueOf("" + currentValue);
							Double minDouble = Double.valueOf(min);
							Double maxDouble = Double.valueOf(max);
							if (currentDouble < minDouble || currentDouble > maxDouble) {
								validCounter -= 1;
							}

						}
						if (colType.equals("class java.util.Date")) {
							SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
							Date currentDate = dateFormat.parse(currentValue + "");
							Date minDate = dateFormat.parse(min);
							Date maxDate = dateFormat.parse(max);
							if (currentDate.compareTo(minDate) < 0 || currentDate.compareTo(maxDate) > 0) {
								validCounter -= 1;
							}

						}
					}
				}
			}
		}

		reader.close();
		if (validCounter == htblColNameValue.size()) {
			return true;
		} else {
			return false;
		}
	}

//	public void insertIntoPage(Table tableObj, int pageIdx, Hashtable<String, Object> htblColNameValue) throws ClassNotFoundException, IOException {
//		boolean inserted = false;
//
//		Page pageObj = (Page) deserializeObject(tableObj.TableName + "/" + tableObj.pages.elementAt(pageIdx).ID + ".ser");
//
//		for (int j = 0; j < pageObj.records.size(); j++) {
//			if (compare(pageObj.records.get(j).get(tableObj.ClusteringKey),
//					htblColNameValue.get(tableObj.ClusteringKey)) > 0) {
//				if (j == 0) {
//					pageObj.minKey = htblColNameValue.get(tableObj.ClusteringKey);
//				}
//				pageObj.records.add(j, htblColNameValue);
//				inserted = true;
//				break;
//			}
//		}
//
//		if (!inserted) {
//			pageObj.records.add(htblColNameValue);
//			pageObj.maxKey = htblColNameValue.get(tableObj.ClusteringKey);
//		}
//
//		pageObj.increaseRecords();
//		pageObj.addKey(htblColNameValue.get(tableObj.ClusteringKey));
//	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ParseException, ClassNotFoundException {

// validating that the record matches the table's data types and number of
// attributes

		boolean flag = validate(strTableName, htblColNameValue);
		if (!flag)
			throw new DBAppException();

		Table tableObj = (Table) deserializeObject("Tables/" + strTableName + ".ser");

// check to see if record contains the clustering key or not

		Set<String> columnNames = htblColNameValue.keySet();

		if (!(columnNames.contains(tableObj.ClusteringKey)))
			throw new DBAppException();

// case where the table has no record yet

		if (tableObj.numOfPages == 0) {
			// System.out.println("first record to ever be inserted");
			tableObj.increaseID();
			Page pageObj = new Page(tableObj.TableName + "" + (tableObj.pageID));
			tableObj.pages.add(pageObj);
			tableObj.increasePage();

			pageObj.addRecord(htblColNameValue);
			pageObj.increaseRecords();

			pageObj.addKey(htblColNameValue.get(tableObj.ClusteringKey));

			pageObj.maxKey = htblColNameValue.get(tableObj.ClusteringKey);
			pageObj.minKey = htblColNameValue.get(tableObj.ClusteringKey);

			serializeObject(pageObj, strTableName + "/" + pageObj.ID + ".ser");
			serializeObject(tableObj, "Tables/" + strTableName + ".ser");
			return;

		}

		else {

			// checks to see if a duplicate primary key exists
			for (int i = 0; i < tableObj.pages.size(); i++) {
				Page pageObj = (Page) deserializeObject(strTableName + "/" + tableObj.pages.get(i).ID + ".ser");
				if (pageObj.primaryKeys.contains(htblColNameValue.get(tableObj.ClusteringKey))) {
					serializeObject(pageObj, strTableName + "/" + tableObj.pages.get(i).ID + ".ser");
					serializeObject(tableObj, "Tables/" + strTableName + ".ser");
					throw new DBAppException();
				}

				serializeObject(pageObj, strTableName + "/" + tableObj.pages.get(i).ID + ".ser");
			}
		}

		int pageIdx = whichPageIdx(tableObj, htblColNameValue);

		if (pageIdx > 0) {
			pageIdx = pageIdx - 1;
		}

		Page pageObj = (Page) deserializeObject(strTableName + "/" + tableObj.pages.elementAt(pageIdx).ID + ".ser");

		if (pageObj.numOfRecords < getMaxRows()) {

			// System.out.println("page is NOT full so there is a spot for
			// "+htblColNameValue.get(tableObj.ClusteringKey));

			// insertIntoPage(tableObj,pageIdx, htblColNameValue);

			boolean inserted = false;

			for (int j = 0; j < pageObj.records.size(); j++) {
				if (compare(pageObj.records.get(j).get(tableObj.ClusteringKey),
						htblColNameValue.get(tableObj.ClusteringKey)) > 0) {
					if (j == 0) {
						pageObj.minKey = htblColNameValue.get(tableObj.ClusteringKey);
					}
					pageObj.records.add(j, htblColNameValue);
					inserted = true;
					break;
				}
			}

			if (!inserted) {
				pageObj.records.add(htblColNameValue);
				pageObj.maxKey = htblColNameValue.get(tableObj.ClusteringKey);
			}

			pageObj.increaseRecords();
			pageObj.addKey(htblColNameValue.get(tableObj.ClusteringKey));
			serializeObject(pageObj, strTableName + "/" + tableObj.pages.elementAt(pageIdx).ID + ".ser");
			serializeObject(tableObj, "Tables/" + strTableName + ".ser");
			return;
		}

		// page is full
		else if (pageObj.numOfRecords == getMaxRows()) {

			// System.out.println("page is full and current clustering key is
			// "+htblColNameValue.get(tableObj.ClusteringKey));

			boolean greaterThanAllKeys = greaterThanAllKeys(pageObj.primaryKeys,
					htblColNameValue.get(tableObj.ClusteringKey));

			if (!greaterThanAllKeys) {
				// System.out.println(htblColNameValue.get(tableObj.ClusteringKey)+ " has a spot
				// in this full page");
				Hashtable<String, Object> lastElement = pageObj.records.remove(getMaxRows() - 1);
				pageObj.primaryKeys.remove(lastElement.get(tableObj.ClusteringKey));

				boolean inserted = false;

				for (int j = 0; j < pageObj.records.size(); j++) {
					if (compare(pageObj.records.get(j).get(tableObj.ClusteringKey),
							htblColNameValue.get(tableObj.ClusteringKey)) > 0) {
						if (j == 0) {
							pageObj.minKey = htblColNameValue.get(tableObj.ClusteringKey);
						}
						pageObj.records.add(j, htblColNameValue);
						inserted = true;
						break;
					}
				}

				if (!inserted) {
					pageObj.records.add(htblColNameValue);
					pageObj.maxKey = htblColNameValue.get(tableObj.ClusteringKey);
				}

				pageObj.addKey(htblColNameValue.get(tableObj.ClusteringKey));
				serializeObject(pageObj, strTableName + "/" + tableObj.pages.elementAt(pageIdx).ID + ".ser");
				serializeObject(tableObj, "Tables/" + strTableName + ".ser");

				insertIntoTable(strTableName, lastElement);
				return;
			}

			else if (greaterThanAllKeys) {
				// System.out.println("has no spot in this page");

				if (pageIdx == tableObj.pages.size() - 1) {
					// System.out.println(" and I am here in the last page trynna make another
					// page");
					tableObj.increaseID();
					Page extraPage = new Page(strTableName + "" + (tableObj.pageID));
					tableObj.pages.add(extraPage);
					tableObj.increasePage();

					extraPage.addRecord(htblColNameValue);
					extraPage.increaseRecords();
					extraPage.addKey(htblColNameValue.get(tableObj.ClusteringKey));
					extraPage.minKey = htblColNameValue.get(tableObj.ClusteringKey);
					extraPage.maxKey = htblColNameValue.get(tableObj.ClusteringKey);

					serializeObject(extraPage, strTableName + "/" + extraPage.ID + ".ser");
					serializeObject(pageObj, strTableName + "/" + tableObj.pages.elementAt(pageIdx).ID + ".ser");
					serializeObject(tableObj, "Tables/" + strTableName + ".ser");

				}

				else {
					pageIdx = pageIdx + 1;

					Page pageObj2 = (Page) deserializeObject(
							strTableName + "/" + tableObj.pages.elementAt(pageIdx).ID + ".ser");

					Hashtable<String, Object> lastElement = pageObj2.records.remove(pageObj2.records.size() - 1);
					pageObj2.primaryKeys.remove(lastElement.get(tableObj.ClusteringKey));

					boolean inserted = false;

					for (int j = 0; j < pageObj2.records.size(); j++) {
						if (compare(pageObj2.records.get(j).get(tableObj.ClusteringKey),
								htblColNameValue.get(tableObj.ClusteringKey)) > 0) {
							if (j == 0) {
								pageObj.minKey = htblColNameValue.get(tableObj.ClusteringKey);
							}
							pageObj2.records.add(j, htblColNameValue);
							inserted = true;
							break;
						}
					}

					if (!inserted) {
						pageObj2.records.add(htblColNameValue);
						pageObj2.maxKey = htblColNameValue.get(tableObj.ClusteringKey);
					}

					pageObj2.addKey(htblColNameValue.get(tableObj.ClusteringKey));
					serializeObject(pageObj, strTableName + "/" + tableObj.pages.elementAt(pageIdx).ID + ".ser");
					serializeObject(pageObj2, strTableName + "/" + tableObj.pages.elementAt(pageIdx).ID + ".ser");
					serializeObject(tableObj, "Tables/" + strTableName + ".ser");

					insertIntoTable(strTableName, lastElement);
					return;

				}
			}

		}
//	serializeObject(tableObj, "Tables/" + strTableName + ".ser");

	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, ClassNotFoundException, IOException {

		Table tableObj = (Table) deserializeObject("Tables/" + strTableName + ".ser");

		// first case: the htblColNameValue hashtable is empty so we just delete all the
		// pages of a table

		if (htblColNameValue.isEmpty()) {

			for (int i = 0; i < tableObj.pages.size(); i++) {
				Page pageObj = (Page) deserializeObject(strTableName + "/" + tableObj.pages.get(i).ID + ".ser");
				delete(strTableName + "/" + tableObj.pages.get(i).ID + ".ser");

			}

			Table newTable = new Table(tableObj.TableName, tableObj.ClusteringKey);
			tableObj.numOfPages = 0;
			delete("Tables/" + strTableName + ".ser");
			serializeObject(newTable, "Tables/" + strTableName + ".ser");

			return;
		}

		// checks to see if all columns we reference in the delete are existing columns
		// in the table or not
		ArrayList<String> tableColumns = getColumns(strTableName);
		Set<String> keyNames = htblColNameValue.keySet();

		for (String key : keyNames) {
			if (!(tableColumns.contains(key))) {
				serializeObject(tableObj, "Tables/" + strTableName + ".ser");
				throw new DBAppException();
			}
		}
		// checks if datatypes are correct
		Hashtable<String, String> type = getType(strTableName);
		for (String key : keyNames) {
			String currentType = "class " + type.get(key);
			String typeToCheck = htblColNameValue.get(key).getClass() + "";
			if (!typeToCheck.equals(currentType)) {

				serializeObject(tableObj, "Tables/" + strTableName + ".ser");
				throw new DBAppException();
			}
		}
//		// checks for max/min violations
//		Hashtable<String, Object> maxHash = getMax(strTableName);
//		Hashtable<String, Object> minHash = getMin(strTableName);
//		displayHash(maxHash);
//		displayHash(minHash);
//		for (String key : keyNames) {
//			Object currentVal = htblColNameValue.get(key);
//			Object currentMax = maxHash.get(key);
//			Object currentMin = minHash.get(key);
//			System.out.println(currentVal);
//			System.out.println(currentMax);
//			System.out.println(currentMin);
//			if (compare(currentVal, currentMin) < 1 || compare(currentVal, currentMax) > 1) {
//				serializeObject(tableObj, "Tables/" + strTableName + ".ser");
//				throw new DBAppException();
//			}
//		}

		// start actual deletion of record by record process

		Vector<Page> pagesToBeDeleted = new Vector<Page>();

		for (int i = 0; i < tableObj.pages.size(); i++) {
			Page pageObj = (Page) deserializeObject(strTableName + "/" + tableObj.pages.get(i).ID + ".ser");
			Vector<Hashtable<String, Object>> recordsToBeDeleted = new Vector<Hashtable<String, Object>>();
			boolean recordsGotDeleted = false;

			for (int j = 0; j < pageObj.records.size(); j++) {
				Hashtable<String, Object> currRecord = pageObj.records.get(j);
				Set<String> recKeys = currRecord.keySet();
				if (recKeys.containsAll(keyNames)) { // you are only interested in records that have values in all the
														// columns that are in the deletion context
					boolean sameValues = sharesValues(currRecord, htblColNameValue);
					if (sameValues) {
						pageObj.removeKey(currRecord.get(tableObj.ClusteringKey));
						pageObj.decreaseRecords();
						recordsToBeDeleted.add(currRecord);
						recordsGotDeleted = true;

					}
				}
			}

			// interested in removing all records that are concerned with the values
			// parameterized
			// so i dont fuck up the loop going over the records of the page
			// if all records got deleted from a page, delete this page
			// plus checking to see if i need to shift up in case a page was full and i
			// deleted from it
			if (recordsGotDeleted) {
				pageObj.records.removeAll(recordsToBeDeleted);
				if (pageObj.numOfRecords == 0) {
					pagesToBeDeleted.add(pageObj);
					pageObj = null;
					tableObj.decreasePage();
				} else {
					updateMaxMin(tableObj.ClusteringKey, pageObj);
				}

			}

			serializeObject(pageObj, strTableName + "/" + tableObj.pages.get(i).ID + ".ser");

		}

		Table newTable = new Table(tableObj.TableName, tableObj.ClusteringKey);

		for (int i = 0; i < tableObj.pages.size(); i++) {
			Page pageObj = (Page) deserializeObject(strTableName + "/" + tableObj.pages.get(i).ID + ".ser");
			if (pageObj != null) {
				newTable.pages.add(tableObj.pages.get(i));
			} else {
				delete(strTableName + "/" + tableObj.pages.get(i).ID + ".ser");
			}
		}

		newTable.numOfPages = tableObj.numOfPages;
		newTable.pageID = tableObj.pageID;

		delete("Tables/" + strTableName + ".ser");
		serializeObject(newTable, "Tables/" + strTableName + ".ser");

	}

	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, ClassNotFoundException, IOException {

		Table tableObj = (Table) deserializeObject("Tables/" + strTableName + ".ser");

		// checks to see if all columns we reference in the update are existing columns
		// in the table or not
		ArrayList<String> tableColumns = getColumns(strTableName);
		Set<String> keyNames = htblColNameValue.keySet();

		for (String key : keyNames) {
			if (!(tableColumns.contains(key))) {
				serializeObject(tableObj, "Tables/" + strTableName + ".ser");
				throw new DBAppException();
			}
		}
		// checks if datatypes are correct
		Hashtable<String, String> type = getType(strTableName);
		for (String key : keyNames) {
			String currentType = "class " + type.get(key);
			String typeToCheck = htblColNameValue.get(key).getClass() + "";
			if (!typeToCheck.equals(currentType)) {

				serializeObject(tableObj, "Tables/" + strTableName + ".ser");
				throw new DBAppException();
			}
		}
//		// checks for min/max violations
//		Hashtable<String, Object> maxHash = getMax(strTableName);
//		Hashtable<String, Object> minHash = getMin(strTableName);
//		for (String key : keyNames) {
//			Object currentVal = htblColNameValue.get(key);
//			Object currentMax = maxHash.get(key);
//			Object currentMin = minHash.get(key);
//			if (compare(currentVal, currentMin) < 1 || compare(currentVal, currentMax) > 1) {
//				serializeObject(tableObj, "Tables/" + strTableName + ".ser");
//				throw new DBAppException();
//			}
//		}

		// throw an exception if i'm trying to update the clustering key
		if (keyNames.contains(tableObj.ClusteringKey)) {
			throw new DBAppException();
		}

		// find the record to update

		Object clusteringKey = parseStringToObject(strClusteringKeyValue);

		Hashtable<String, Object> recordToBeUpdated = new Hashtable<String, Object>();
		Set<String> keys = htblColNameValue.keySet();

		outerloop: for (int i = 0; i < tableObj.pages.size(); i++) {
			Page pageObj = (Page) deserializeObject(strTableName + "/" + tableObj.pages.get(i).ID + ".ser");
			if (pageObj.primaryKeys.contains(clusteringKey)) {
				for (Hashtable<String, Object> hashtable : pageObj.records) {
					if (hashtable.get(tableObj.ClusteringKey).equals(clusteringKey)) {
						recordToBeUpdated = hashtable;
						for (String key : keys) {
							Object value = htblColNameValue.get(key);
							recordToBeUpdated.put(key, value);
						}
						serializeObject(pageObj, strTableName + "/" + tableObj.pages.get(i).ID + ".ser");
						break outerloop;
					}
				}
			}

			serializeObject(pageObj, strTableName + "/" + tableObj.pages.get(i).ID + ".ser");
		}

		serializeObject(tableObj, "Tables/" + strTableName + ".ser");
	}


	public int whichPageIdx(Table tableObj, Hashtable<String, Object> htblColNameValue)
			throws ClassNotFoundException, IOException {

		Vector<Object> pageMin = new Vector<Object>();
		Object key = htblColNameValue.get(tableObj.ClusteringKey);

		for (int i = 0; i < tableObj.pages.size(); i++) {
			Page pageObj = (Page) deserializeObject(tableObj.TableName + "/" + tableObj.pages.get(i).ID + ".ser");
			pageMin.add(pageObj.minKey);
			serializeObject(pageObj, tableObj.TableName + "/" + tableObj.pages.get(i).ID + ".ser");
		}

		int low = 0;
		int high = pageMin.size() - 1;

		while (low <= high) {
			int mid = (low + high) / 2;
			Object midObj = pageMin.get(mid);
			int cmp = compare(key, midObj);
			if (cmp < 0) {
				high = mid - 1;
			} else if (cmp > 0) {
				low = mid + 1;
			} else {
				// if newObj has the same value as midObj, insert it after midObj
				return mid + 1;
			}
		}

		return low;

	}

	public void updateMaxMin(String clusteringKey, Page pageObj) {
		pageObj.minKey = pageObj.records.get(0).get(clusteringKey);
		int lastIndex = pageObj.records.size() - 1;
		pageObj.maxKey = pageObj.records.get(lastIndex).get(clusteringKey);
	}

	public boolean sharesValues(Hashtable<String, Object> currRecord, Hashtable<String, Object> htblColNameValue) {
		Set<String> sharedKeys = new HashSet<String>(currRecord.keySet());
		sharedKeys.retainAll(htblColNameValue.keySet()); // retain only the keys that are in both hashtables

		boolean sameValues = true;
		for (String key : sharedKeys) {
			if (!currRecord.get(key).equals(htblColNameValue.get(key))) {
				sameValues = false;
				break;
			}
		}

		return sameValues;
	}


	public void delete(String path) {
		File f = new File(path);
		f.delete();
	}

	public void displayHash(Hashtable<String, Object> records) {
		records.entrySet().forEach(entry -> {
			System.out.println(entry.getKey() + "->" + entry.getValue());
		});
	}

	public boolean greaterThanAllKeys(Vector<Object> pageKeys, Object key) {

		int num = 0;

		for (int i = 0; i < pageKeys.size(); i++) {
			if (compare(pageKeys.get(i), key) < 0)
				num++;
		}

		if (num == pageKeys.size())
			return true;
		else
			return false;
	}

	public int compare(Object key1, Object key2) {

		if (key1 instanceof java.lang.Integer) {
			return ((Integer) key1).compareTo((Integer) key2);
		} else if (key1 instanceof java.lang.Double) {
			return ((Double) key1).compareTo((Double) key2);
		} else if (key1 instanceof java.util.Date) {
			return ((Date) key1).compareTo((Date) key2);
		} else {
			return ((String) key1).compareTo((String) key2);
		}
	}

	public static int getMaxRows() throws IOException {
		Properties prop = new Properties();
		InputStream input = null;

		input = new FileInputStream("src\\project1\\resources\\DBApp.config");

		// load a properties file
		prop.load(input);

		return Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));

	}

	public void serializeObject(Object o, String path) throws IOException {
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(o);
		out.close();
		fileOut.close();
	}

	public Object deserializeObject(String path) throws IOException, ClassNotFoundException {
		FileInputStream fileIn = new FileInputStream(path);
		ObjectInputStream objectIn = new ObjectInputStream(fileIn);
		Object o = objectIn.readObject();
		objectIn.close();
		fileIn.close();
		return o;
	}

	public ArrayList<String> getColumns(String tableName) {
		ArrayList<String> columnNames = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader("metadata.csv"))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				if (values[0].equals(tableName)) {
					columnNames.add(values[1]); // add the column name to the list
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return columnNames;
	}

	public Hashtable<String, Object> getMax(String tableName) {
		Hashtable<String, Object> maxHash = new Hashtable<String, Object>();
		try (BufferedReader br = new BufferedReader(new FileReader("metadata.csv"))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				if (values[0].equals(tableName)) {
					maxHash.put(values[1], values[7]); // add the column name to the list
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return maxHash;
	}

	public Hashtable<String, String> getType(String tableName) {
		Hashtable<String, String> type = new Hashtable<String, String>();
		try (BufferedReader br = new BufferedReader(new FileReader("metadata.csv"))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				if (values[0].equals(tableName)) {
					type.put(values[1], values[2]); // add the column name to the list
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return type;
	}

	public Hashtable<String, Object> getMin(String tableName) {
		Hashtable<String, Object> minHash = new Hashtable<String, Object>();
		try (BufferedReader br = new BufferedReader(new FileReader("metadata.csv"))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				if (values[0].equals(tableName)) {
					minHash.put(values[1], values[6]); // add the column name to the list
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return minHash;
	}

	public void displayTable(String strTableName) throws ClassNotFoundException, IOException {
		Table tableObj = (Table) deserializeObject("Tables/" + strTableName + ".ser");

		System.out.println("the number of pages this table has " + tableObj.pages.size());

		for (int j = 0; j < tableObj.pages.size(); j++) {
			Page pageObj = (Page) deserializeObject(strTableName + "/" + tableObj.pages.get(j).ID + ".ser");
			for (int i = 0; i < pageObj.records.size(); i++) {
				System.out.println("i am in page " + j);
				System.out.println("with record number " + i);
				displayHash(pageObj.records.get(i));
			}

		}

	}

	public static Object parseStringToObject(String str) {
		try {
			// Try parsing as integer
			return Integer.parseInt(str);
		} catch (NumberFormatException e) {
			try {
				// Try parsing as double
				return Double.parseDouble(str);
			} catch (NumberFormatException e2) {
				try {
					// Try parsing as date
					SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
					Date date = format.parse(str);
					return date;
				} catch (ParseException e3) {
					// Default to string
					return str;
				}
			}
		}
	}

	public static void main(String[] args) throws DBAppException, IOException, ParseException, ClassNotFoundException {

		DBApp test = new DBApp();

		String tableName = "Doctor";

		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("salary", "java.lang.Integer");

		Hashtable<String, String> min = new Hashtable<String, String>();
		min.put("id", "0");
		min.put("name", "A");
		min.put("salary", "10000");

		Hashtable<String, String> max = new Hashtable<String, String>();
		max.put("id", "1000");
		max.put("name", "ZZZZZ");
		max.put("salary", "40000");

		String tableName1 = "student";

		Hashtable<String, String> htblColNameType1 = new Hashtable<String, String>();
		htblColNameType1.put("id", "java.lang.Integer");
		htblColNameType1.put("name", "java.lang.String");
		htblColNameType1.put("age", "java.lang.Integer");

		Hashtable<String, String> min1 = new Hashtable<String, String>();
		min1.put("id", "0");
		min1.put("name", "A");
		min1.put("age", "18");

		Hashtable<String, String> max1 = new Hashtable<String, String>();
		max1.put("id", "1000");
		max1.put("name", "ZZZZZ");
		max1.put("age", "40");

		test.createTable(tableName, "id", htblColNameType, min, max);
		test.createTable(tableName1, "id", htblColNameType1, min1, max1);

		Hashtable<String, Object> htblColNameValue1 = new Hashtable<String, Object>();
		htblColNameValue1.put("id", 1);
		htblColNameValue1.put("name", "Habiba");
		htblColNameValue1.put("salary", 20000);

		Hashtable<String, Object> htblColNameValue2 = new Hashtable<String, Object>();
		htblColNameValue2.put("id", 11);
		htblColNameValue2.put("name", "Karim");
		htblColNameValue2.put("salary", 20000);

		Hashtable<String, Object> htblColNameValue3 = new Hashtable<String, Object>();
		htblColNameValue3.put("id", 6);
		htblColNameValue3.put("name", "Sara");
		htblColNameValue3.put("salary", 25000);

		Hashtable<String, Object> htblColNameValue4 = new Hashtable<String, Object>();
		htblColNameValue4.put("id", 17);
		htblColNameValue4.put("name", "Hamza");
		htblColNameValue4.put("salary", 40000);

		Hashtable<String, Object> htblColNameValue5 = new Hashtable<String, Object>();
		htblColNameValue5.put("id", 34);
		htblColNameValue5.put("name", "Nadia");
		htblColNameValue5.put("salary", 35000);

		Hashtable<String, Object> htblColNameValue6 = new Hashtable<String, Object>();
		htblColNameValue6.put("id", 9);
		htblColNameValue6.put("name", "Samar");
		htblColNameValue6.put("salary", 25000);

		Hashtable<String, Object> htblColNameValue7 = new Hashtable<String, Object>();
		htblColNameValue7.put("id", 15);
		htblColNameValue7.put("name", "Ali");
		htblColNameValue7.put("salary", 35000);

		Hashtable<String, Object> htblColNameValue8 = new Hashtable<String, Object>();
		htblColNameValue8.put("id", 22);
		htblColNameValue8.put("name", "Samar");
		htblColNameValue8.put("salary", 27000);

		Hashtable<String, Object> htblColNameValue20 = new Hashtable<String, Object>();
		htblColNameValue20.put("id", 20);
		htblColNameValue20.put("name", "Mohamed");
		htblColNameValue20.put("salary", 27000);

		test.insertIntoTable(tableName, htblColNameValue7);
		test.insertIntoTable(tableName, htblColNameValue4);
		test.insertIntoTable(tableName, htblColNameValue1);
		test.insertIntoTable(tableName, htblColNameValue2);
		test.insertIntoTable(tableName, htblColNameValue5);
		test.insertIntoTable(tableName, htblColNameValue3);
		test.insertIntoTable(tableName, htblColNameValue6);

		test.displayTable(tableName);

		Hashtable<String, Object> toDelete = new Hashtable<String, Object>();
		toDelete.put("id", 17);
		// toDelete.put("name", "Samar");
		// toDelete.put("salary", 27000);

		Hashtable<String, Object> toDelete1 = new Hashtable<String, Object>();
		toDelete1.put("id", 6);

		Hashtable<String, Object> toDelete2 = new Hashtable<String, Object>();
		toDelete2.put("id", 34);

		test.deleteFromTable(tableName, toDelete1);
		test.deleteFromTable(tableName, toDelete2);
		test.deleteFromTable(tableName, toDelete);

		System.out.println("// another hashtabes \n");

		test.displayTable(tableName);

		test.insertIntoTable(tableName, htblColNameValue5);

		System.out.println("// another hashtabes \n");
		test.displayTable(tableName);

		Hashtable<String, Object> htblColNameValue14 = new Hashtable<String, Object>();
		htblColNameValue14.put("id", 14);
		htblColNameValue14.put("name", "Louji");
		htblColNameValue14.put("salary", 35000);

		Hashtable<String, Object> htblColNameValue10 = new Hashtable<String, Object>();
		htblColNameValue10.put("id", 7);
		htblColNameValue10.put("name", "Shosho");
		htblColNameValue10.put("salary", 27000);

		Hashtable<String, Object> htblColNameValue11 = new Hashtable<String, Object>();
		htblColNameValue11.put("id", 3);
		htblColNameValue11.put("name", "Hamza");
		htblColNameValue11.put("salary", 27000);
		
		test.insertIntoTable(tableName, htblColNameValue14);
		
		System.out.println("// another hashtabes \n");
		test.displayTable(tableName);

		test.insertIntoTable(tableName, htblColNameValue10);
		test.insertIntoTable(tableName, htblColNameValue11);

		System.out.println("// another hashtabes \n");

		test.displayTable(tableName);
////
//		Hashtable<String, Object> toDelete1 = new Hashtable<String, Object>();
//		toDelete1.put("name", "Habiba");
//
//		Hashtable<String, Object> toDelete2 = new Hashtable<String, Object>();
//		toDelete2.put("name", "Karim");
//
//		Hashtable<String, Object> toDelete3 = new Hashtable<String, Object>();
//		toDelete3.put("name", "Sara");
//
//		test.deleteFromTable(tableName, toDelete1);
//		test.deleteFromTable(tableName, toDelete2);
//		test.deleteFromTable(tableName, toDelete3);
//
//		System.out.println("// another hashtabes \n");
//
//		test.displayTable(tableName);
//
//		Hashtable<String, Object> toUpdate = new Hashtable<String, Object>();
//		toUpdate.put("name", "mada");
//
//		test.updateTable(tableName, "20", toUpdate);
//
//		System.out.println("// another hashtabes \n");
//
//		test.displayTable(tableName);

	}
}
