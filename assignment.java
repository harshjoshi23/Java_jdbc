package com.milestone1;

import java.sql.Connection;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
//import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
//import java.sql.DriverManager;
//import java.sql.ResultSet;
//import java.sql.Statement;


public class assignment {
	
	static final String Driver = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost/milestone1";;
	static final String USER = "root";
	static final String PASS = "root";

	public static void writeIntoDB(ArrayList<invoice_details> data, int batchSize) {
		String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
		String DB_URL = "jdbc:mysql://localhost:3306/invoice_details ";
		String USER = "root";
		String PASS = "root";

		Connection dbCon = null;
		PreparedStatement pst = null;
		String query1 = "INSERT INTO invoice_details VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"; // 17 ?

		try {
			Class.forName(JDBC_DRIVER);
			dbCon = DriverManager.getConnection(DB_URL, USER, PASS);
			dbCon.setClientInfo("rewriteBatchedStatements", "true");

			pst = dbCon.prepareStatement(query1);

			int count = 0; // for batching process
			System.out.println("Db started");
			for (invoice_details dataInvoice : data) {
				pst.setString(1, dataInvoice.getBusiness_code());
				pst.setString(2, dataInvoice.getCust_number());
				pst.setString(3, dataInvoice.getName_customer());
				if (dataInvoice.getClear_date() != null) {
					pst.setDate(4, new java.sql.Date(dataInvoice.getClear_date().getTime()));
				}

				pst.setInt(5, dataInvoice.getBuisness_year());
				pst.setLong(6, dataInvoice.getDoc_id());
				pst.setDate(7, new java.sql.Date(dataInvoice.getPosting_date().getTime()));
				pst.setDate(8, new java.sql.Date(dataInvoice.getDocument_create_date().getTime()));
				pst.setDate(9, new java.sql.Date(dataInvoice.getDue_in_date().getTime()));
				pst.setString(10, dataInvoice.getInvoice_currency());
				pst.setString(11, dataInvoice.getDocument_type());
				pst.setInt(12, dataInvoice.getPosting_id());
				pst.setString(13, dataInvoice.getArea_business());
				pst.setDouble(14, dataInvoice.getTotal_open_amount());
				pst.setDate(15, new java.sql.Date(dataInvoice.getBaseline_create_date().getTime()));
				pst.setString(16, dataInvoice.getCust_payment_terms());
				pst.setLong(17, dataInvoice.getInvoice_id());
				pst.setInt(18, dataInvoice.getIsOpen());

				pst.addBatch();
				count += 1;

				if (count % batchSize == 0) {
					// executing the query
					pst.executeBatch();
				}

				

			}
			dbCon.close();
			System.out.println("Complted");
//			st.close();
			pst.close();

		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (pst != null)
					pst.close();
			} catch (SQLException se2) {
//				nothing we can do
			}
			try {
				if (dbCon != null)
					dbCon.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}

	}

	public static void connectToDb(ArrayList<invoice_details> A1) {
		Connection conn = null;
		Statement stmt = null;
		try {

			Class.forName("com.mysql.cj.jdbc.Driver");
			// STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			conn.setClientInfo("rewriteBatchedStatements", "true");

			// STEP 4: Execute a query
			stmt = conn.createStatement();
			String query = "insert into `invoice_details` values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			System.out.println("Connected");

			PreparedStatement ps = null;
			ps = conn.prepareStatement(query);

			long start = System.currentTimeMillis();
			for (invoice_details j : A1) {
				ps.setString(1, j.getBusiness_code());
				ps.setString(2, j.getCust_number());
				ps.setString(3, j.getName_customer());
				if (j.getClear_date() != null) {
					java.sql.Date cDate = new java.sql.Date(j.getClear_date().getTime());
					ps.setDate(4, cDate);
				} else {
					ps.setNull(4, Types.NULL);
				}
				ps.setInt(5, j.getBuisness_year());
				ps.setLong(6, j.getDoc_id());
				java.sql.Date posDate = new java.sql.Date(j.getPosting_date().getTime());
				ps.setDate(7, posDate);
				java.sql.Date dDate = new java.sql.Date(j.getDocument_create_date().getTime());
				ps.setDate(8, dDate);
				java.sql.Date dueDate = new java.sql.Date(j.getDue_in_date().getTime());
				ps.setDate(9, dueDate);
				ps.setString(10, j.getInvoice_currency());
				ps.setString(11, j.getDocument_type());
				ps.setInt(12, j.getPosting_id());
				ps.setString(13, j.getArea_business());
				ps.setDouble(14, j.getTotal_open_amount());
				java.sql.Date bDate = new java.sql.Date(j.getBaseline_create_date().getTime());
				ps.setDate(15, bDate);
				ps.setString(16, j.getCust_payment_terms());
				int len = String.valueOf(j.getInvoice_id()).length();
				if (len != 0) {
					ps.setLong(17, j.getInvoice_id());
				}

				ps.setInt(18, j.getIsOpen());

				ps.addBatch();

			}
			ps.executeBatch();
			

			System.out.println("Completed Successfully!");

		} catch (Exception e) {
			System.out.println(e.toString());
		}

	}

	public static void main(String[] args) {
		String path = "D:/repos/hrc_react/1806381.csv";
		SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
		try {
			String line = "";
			ArrayList<invoice_details> arr = new ArrayList<>();
			BufferedReader br = new BufferedReader(new FileReader(path));
			br.readLine();
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				invoice_details h1 = new invoice_details();
				h1.setBusiness_code(values[0]);
				h1.setCust_number(values[1]);
				h1.setName_customer(values[2]);
				if (values[3].length() != 0) {
					Date value = formatter1.parse(values[3]);
					h1.setClear_date(value);
				}

				double year = Double.parseDouble(values[4]);
				h1.setBuisness_year((int) year);
				double id = Double.parseDouble(values[5]);
				h1.setDoc_id((long) id);

				Date value1 = formatter2.parse(values[6]);
				h1.setPosting_date(value1);
				Date value2 = formatter.parse(values[7]);
				h1.setDocument_create_date(value2);
				// h1.setDocument_create_date_1(values[8]);
				Date value4 = formatter.parse(values[9]);
				h1.setDue_in_date(value4);

				h1.setInvoice_currency(values[10]);
				h1.setDocument_type(values[11]);

				double post = Double.parseDouble(values[12]);
				h1.setPosting_id((int) post);
				System.out.println(values[10]);
				h1.setArea_business(values[13]);

				h1.setTotal_open_amount(Double.parseDouble(values[14]));

				Date value5 = formatter.parse(values[15]);
				h1.setBaseline_create_date(value5);

				h1.setCust_payment_terms(values[16]);
				if (values[17].length() != 0) {
					double invoice_id = Double.parseDouble(values[17]);
					h1.setInvoice_id((long) invoice_id);
				}

				double open = Double.parseDouble(values[18]);
				h1.setIsOpen((int) open);

				arr.add(h1);

			}
			br.close();
			connectToDb(arr);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
		}

	}
}