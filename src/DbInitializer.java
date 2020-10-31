import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DbInitializer {

    private static final String JDBC_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String DB_URL = "jdbc:derby:DvDRentalDb;create=true";
    private static Connection conn = null;
    private static Statement stmt = null;

    public static void Initialize() {
        createConnection();
        seedDatabase();
        close();
    }

    private static void createConnection() {
        try {
            System.out.println("Connecting to database...");
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL);
            System.out.println("Connection successful!");
        } catch (Exception except) {
            except.printStackTrace();
        }
    }

    private static void seedDatabase() {
        System.out.println("Creating database...");
        try {
            // TODO Must have a statement to check if the database is already seeded
            DatabaseMetaData metadata = conn.getMetaData();
            ResultSet resultSet = metadata.getTables(null, null, null, new String[]{"TABLE"});
            stmt = conn.createStatement();
            List<String> tables = new ArrayList<>();

            while (resultSet.next()) {
                tables.add(resultSet.getString("TABLE_NAME").toLowerCase());
            }

            if (tables.contains("rental")) {
                stmt.executeUpdate("alter table rental drop constraint custNumber_fk");
                stmt.executeUpdate("alter table rental drop constraint dvdNumber_fk");
                stmt.executeUpdate("drop table rental");
            }

            if (tables.contains("customer")) {
                stmt.executeUpdate("drop table customer");
            }

            if (tables.contains("dvd")) {
                stmt.executeUpdate("drop table dvd");
            }

            stmt.executeUpdate("create table customer(custNumber int not null, firstName varchar(40), surname varchar(40)," +
                    "phoneNum varchar(40), credit decimal(10, 2), canRent boolean, primary key (custNumber))");

            stmt.executeUpdate("create table dvd(dvdNumber int not null, title varchar(40), category varchar(40)," +
                    "price double, newRelease boolean, availableForRent boolean, primary key (dvdNumber))");

            stmt.executeUpdate("create table rental(rentalNumber int not null, dateRented varchar(40), dateReturned " +
                    "varchar(40), totalPenaltyCost decimal(10, 2), custNumber int, dvdNumber int, primary key (rentalNumber), " +
                    "constraint custNumber_fk FOREIGN KEY (custNumber) REFERENCES customer(custNumber), constraint dvdNumber_fk " +
                    "FOREIGN KEY (dvdNumber) REFERENCES dvd(dvdNumber))");

            readAndInsertData();

            System.out.println("Database successfully seeded!!!");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void readAndInsertData() {
        try (
                InputStream file = new FileInputStream("./src/Customers.ser");
                InputStream buffer = new BufferedInputStream(file);
                ObjectInput input = new ObjectInputStream(buffer)
        ) {
            while (true) {
                //deserialize the List
                Customer customer = (Customer) input.readObject();

                PreparedStatement statement = conn.prepareStatement("insert into customer values (?, ?, ?, ?, ?, ?)");
                statement.setInt(1, customer.getCustNumber());
                statement.setString(2, customer.getName());
                statement.setString(3, customer.getSurname());
                statement.setString(4, customer.getPhoneNum());
                statement.setDouble(5, customer.getCredit());
                statement.setBoolean(6, customer.canRent());

                statement.executeUpdate();
            }

        } catch (EOFException ignored) {
        } catch (ClassNotFoundException | IOException ex) {
            System.err.println(ex.getMessage());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try (
                InputStream file = new FileInputStream("./src/Movies.ser");
                InputStream buffer = new BufferedInputStream(file);
                ObjectInput input = new ObjectInputStream(buffer)
        ) {
            while (true) {
                //deserialize the List
                DVD dvd = (DVD) input.readObject();

                PreparedStatement statement = conn.prepareStatement("insert into dvd values (?, ?, ?, ?, ?, ?)");
                statement.setInt(1, dvd.getDvdNumber());
                statement.setString(2, dvd.getTitle());
                statement.setString(3, dvd.getCategory());
                statement.setDouble(4, dvd.getPrice());
                statement.setBoolean(5, dvd.isNewRelease());
                statement.setBoolean(6, dvd.isAvailable());

                statement.executeUpdate();
            }

        } catch (EOFException ignored) {
        } catch (ClassNotFoundException | IOException ex) {
            System.err.println(ex.getMessage());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try (
                InputStream file = new FileInputStream("./src/Rental.ser");
                InputStream buffer = new BufferedInputStream(file);
                ObjectInput input = new ObjectInputStream(buffer)
        ) {
            while (true) {
                //deserialize the List
                Rental rental = (Rental) input.readObject();

                PreparedStatement statement = conn.prepareStatement("insert into rental values (?, ?, ?, ?, ?, ?)");
                statement.setInt(1, rental.getRentalNumber());
                statement.setString(2, rental.getDateRented());
                statement.setString(3, rental.getDateReturned());
                statement.setDouble(4, rental.getTotalPenaltyCost());
                statement.setInt(5, rental.getCustNumber());
                statement.setInt(6, rental.getDvdNumber());

                statement.executeUpdate();

                System.out.println(rental);
            }

        } catch (EOFException ignored) {
        } catch (ClassNotFoundException | IOException ex) {
            System.err.println(ex.getMessage());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private static void close() {
        try {
            System.out.println("Closing database");
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException sqlExcept) {
            System.err.println("An error occurred while closing the database");
            System.err.println(sqlExcept.getMessage());
        }
    }
}
