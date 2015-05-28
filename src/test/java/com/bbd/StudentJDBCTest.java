package com.bbd;

import org.apache.derby.tools.ij;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class StudentJDBCTest {

	public static final String DB_URL = "jdbc:derby:memory:StudentsDB;create=true";
	public static final String USER = "";
	public static final String PASS = "";

	private static String mDDLFileName = "/sql/createStudentsDB_DERBY.sql";

	@BeforeClass
	public static void initTest() throws Exception {
		Connection conn = null;

		System.out.println("Connecting to database...");
		conn = DriverManager.getConnection(DB_URL, USER, PASS);

		initDB(conn);

	}

	/**
	 * Prepares the database by creating the tables and setting up test data.
	 */
	private static void initDB(final Connection connection) throws Exception {
		
		//build the database
		ij.runScript(connection,
				StudentJDBCTest.class.getResourceAsStream(mDDLFileName),
				"UTF-8", System.out, "UTF-8");

		// Load the test datasets in the database
		IDatabaseConnection dbUnitConnection = new DatabaseConnection(
				connection);
		IDataSet dataset = new FlatXmlDataSetBuilder().build(Thread
				.currentThread().getContextClassLoader()
				.getResourceAsStream("students-datasets.xml"));
		DatabaseOperation.CLEAN_INSERT.execute(dbUnitConnection, dataset);
	}
	
	@Test
	public void testSelect() {

		try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
				Statement stmt = conn.createStatement();) {
			String sql = "SELECT id, first_name, last_name, birth_date FROM students";
			ResultSet rs = stmt.executeQuery(sql);

			if (!rs.next() ) {
			    System.out.println("no data");
			    fail();
			}

		} catch (SQLException se) {
			se.printStackTrace();
		}
	}
	
	@Test
	public void testSelectOneStudent() {

		try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
				Statement stmt = conn.createStatement();) {
			String sql = "SELECT id, first_name, last_name, birth_date FROM students WHERE last_name = 'Barrett'";
			ResultSet rs = stmt.executeQuery(sql);
			int num_rows = 0;
			
			while (rs.next()){
			    num_rows++;
			}
			
			assertEquals(num_rows,1);

		} catch (SQLException se) {
			se.printStackTrace();
		}
	}

	@Test
	public void testInsertStudent() {

		try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
			conn.setAutoCommit(false);
			try (Statement stmt = conn.createStatement();) {
				String sql = "INSERT INTO STUDENTS(LAST_NAME, FIRST_NAME, BIRTH_DATE, PHONE_NUMBER) "
						+ "VALUES ('DOE', 'JOHN', '1978-10-10', '00-0000-0000')";
				System.out.println(sql);
				int rowsInserted = stmt.executeUpdate(sql);
				
				assertEquals(rowsInserted, 1);

				conn.commit();
			} catch (SQLException ex) {
				conn.rollback();
				conn.setAutoCommit(true);
				throw ex;
			}

			conn.setAutoCommit(true);
		} catch (SQLException se) {
			se.printStackTrace();
		}
	}

	@Test
	public void testUpdateStudent() {

		try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
			conn.setAutoCommit(false);
			try (Statement stmt = conn.createStatement();) {
				String sql = "UPDATE STUDENTS SET PHONE_NUMBER = '00-0000-0001' WHERE PHONE_NUMBER ='00-0000-0000'";

				System.out.println(sql);
				int rowsUpdated = stmt.executeUpdate(sql);
				assertEquals(rowsUpdated, 6);

				conn.commit();
			} catch (SQLException ex) {
				conn.rollback();
				conn.setAutoCommit(true);
				throw ex;
			}

			conn.setAutoCommit(true);
		} catch (SQLException se) {
			se.printStackTrace();
		}
	}

	@Test
	public void testParametrized() {

		try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
			String sql = "SELECT * FROM students WHERE last_name = ?";
			try (PreparedStatement stmt = conn.prepareStatement(sql);) {
				stmt.setString(1, "Barrett");

				ResultSet rs = stmt.executeQuery();

				int num_rows = 0;
				
				while (rs.next()){
				    num_rows++;
				}
				
				assertEquals(num_rows,1);

			} catch (SQLException ex) {
				throw ex;
			}

		} catch (SQLException se) {
			se.printStackTrace();
		}
	}


}
