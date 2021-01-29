package assignment6;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;

public class jdbc {
	
	
	private Connection conn() {
		String url="jdbc:sqlite:C:\\Users\..\Chinook_Sqlite.sqlite";
		Connection conn=null;
		try {
			conn=DriverManager.getConnection(url);
			return conn;
		} catch (SQLException e) {
			e.printStackTrace();}	
			return conn;
	}
	
	//Display information about all the tables in the database	
	private void printColumnNames() {			
	try {
		DatabaseMetaData dbMetaData = this.conn().getMetaData();
		ResultSet rsTables = dbMetaData.getTables(null, null, null, new String[] {"TABLE"});
		System.out.println("Columns names:\n");
		while (rsTables.next()) {
			String table_name=rsTables.getString("TABLE_NAME");
			System.out.print(table_name + ":");
			ResultSet rsColumns=dbMetaData.getColumns(null, null, table_name, null);
			while(rsColumns.next()) 
				System.out.print(rsColumns.getString("COLUMN_NAME")+",");
			System.out.print("\n");
			
			this.conn().close();
		}}catch (SQLException e) {
				e.printStackTrace();}	
	}
	//Find all the Blues songs by Eric Clapton
	private void EricClaptonBluesSongs() {
		try{
			String sql="SELECT Track.Name FROM Track,Genre,Artist,Album WHERE Artist.Name='Eric Clapton' "
					+ "and Track.GenreId=Genre.GenreId and Track.AlbumId=Album.AlbumId and Album.ArtistId=Artist.ArtistId "
					+ "ORDER BY Track.Name";
			
			Statement st = this.conn().createStatement();
			ResultSet query = st.executeQuery(sql);
			
			System.out.println();
			System.out.print("Eric Clapton Blues songs:");
			while(query.next()) 
				System.out.print(query.getString("Name")+",");
			
			this.conn().close();
		}catch (SQLException e) {
			e.printStackTrace();}
	}
	//Find all the genres sung by Amy Winehouse
	private void AmyWinehouseGenres() {
		try {
			String sql="SELECT	DISTINCT Genre.Name From Genre,Artist,Track,Album WHERE Artist.Name='Amy Winehouse' " 
					+" and Track.GenreId=Genre.GenreId and Track.AlbumId=Album.AlbumId and Album.ArtistId=Artist.ArtistId ";
			Statement st = this.conn().createStatement();
			ResultSet query = st.executeQuery(sql);
			System.out.println();
			System.out.print("\nAmy Winehouse genres:");
			while(query.next()) 
				System.out.print(query.getString("Name")+",");
			this.conn().close();
		}catch (SQLException e) {
			e.printStackTrace();}
	}
	//Find all the songs starting with “The”
	private void SongsStartingWithThe() {
		try {
			System.out.println();
			System.out.print("\nSongs starting with 'The':\n");
			Statement st = this.conn().createStatement();
			ResultSet query= st.executeQuery("SELECT Name from Track where Name Like 'the %'");
			while(query.next()) 
				System.out.println(query.getString("Name"));
			
			this.conn().close();
		}catch (SQLException e) {
			e.printStackTrace();}
	}
	//Create a table Ticket
	private void CreateTicketTable() {
		try {
			String sql="CREATE TABLE IF NOT EXISTS Ticket (\n"
					+ "TicketId integer ,\n"
					+"ConcertLocation varchar(50) NOT NULL,\n"
					+"ConcertDate date NOT NULL,\n"
					+"ArtistId integer,\n"
					+"Price real,\n"
					+"isAvailable boolean,\n"
					+"CONSTRAINT greaterThanZero CHECK (Price >0)"
					+"PRIMARY KEY (TicketId, ConcertLocation, ConcertDate)"
					+"FOREIGN KEY (ArtistId) references Artist"
					+");";
			Statement st = this.conn().createStatement();
			st.execute(sql);
			
			this.conn().close();
		}catch (SQLException e) {
			e.printStackTrace();}
	}
	//Populate the Ticket table
	private void PopulateTicketTable() {
		try {
			Connection conn=this.conn();
			String sql="SELECT ArtistId FROM Artist WHERE Name='Queen'";
			Statement st = conn.createStatement();
			ResultSet query=st.executeQuery(sql);
			int l=query.getInt("ArtistId");
			
			 sql = "DELETE FROM Ticket";
			 st.executeUpdate(sql);
			 
			String sql2="INSERT INTO Ticket (TicketId,ConcertLocation,ConcertDate,ArtistId,Price,isAvailable) VALUES (?,?,?,?,?,?)";
			PreparedStatement pstmt=conn.prepareStatement(sql2);
			for(int i=1;i<=1000;i++) {
				pstmt.setInt(1, i);
				String s="London";
				pstmt.setString(2,s);
					Calendar cal = Calendar.getInstance();
					cal.set(Calendar.YEAR, 2019);
					cal.set(Calendar.MONTH, 6); 
					cal.set(Calendar.DAY_OF_MONTH, 1);
					Date date = new Date(cal.getTimeInMillis());					
				pstmt.setDate(3, date);		
				pstmt.setInt(4,l);
				double d=250.25;
				pstmt.setDouble(5, d);
				pstmt.setBoolean(6, true);
				pstmt.addBatch();	
			}
			pstmt.executeBatch();

			 sql="SELECT TicketId,ConcertLocation,ConcertDate,ArtistId,Price,isAvailable FROM Ticket WHERE TicketId<=5";
			 query=st.executeQuery(sql);
			 System.out.println("\n\nFirst 5 rows of Ticket table");
			 System.out.println("TicketId\tConcertLocation\tConcertDate\tArtistId\tPrice\tisAvailable");
			 while(query.next()) {
				 System.out.print(query.getInt("TicketId")+"\t\t");
				 System.out.print(query.getString("ConcertLocation")+"\t\t");
				 System.out.print(query.getDate("ConcertDate")+"\t");
				 System.out.print(query.getInt("ArtistId")+"\t\t");
				 System.out.print(query.getDouble("Price")+"\t");
				 System.out.print(query.getBoolean("isAvailable")+"\n");
			 }
			
			 query.close();
			this.conn().close();
		}catch (SQLException e) {
			e.printStackTrace();}
	}
	public static void main( String[] args) {
		jdbc j=new jdbc();
		j.printColumnNames();
		j.EricClaptonBluesSongs();
		j.AmyWinehouseGenres();
		j.SongsStartingWithThe();
		j.CreateTicketTable();
		j.PopulateTicketTable();
	}
}		
