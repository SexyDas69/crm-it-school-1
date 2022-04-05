package kg.itschool.crm.dao.impl;

import kg.itschool.crm.dao.MentorDao;
import kg.itschool.crm.dao.daoutil.Log;
import kg.itschool.crm.model.Manager;
import kg.itschool.crm.model.Mentor;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MentorDaoImpl implements MentorDao {

    public MentorDaoImpl() {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {  // api:driver://host:port/database_name
            Log.info(this.getClass().getSimpleName() + " MentorDaoImpl()", Connection.class.getSimpleName(), "Establishing connection");
            connection = getConnection();

            String ddlQuery = "CREATE TABLE IF NOT EXISTS tb_mentors(" +
                    "id           BIGSERIAL, " +
                    "first_name   VARCHAR(50)  NOT NULL, " +
                    "last_name     VARCHAR(50) NOT NULL, " +
                    "email        VARCHAR(100) NOT NULL UNIQUE, " +
                    "phone_number CHAR(13)     NOT NULL, " +
                    "salary       MONEY        NOT NULL, " +
                    "dob          DATE         NOT NULL CHECK(dob < NOW()), " +
                    "date_created TIMESTAMP    NOT NULL DEFAULT NOW(), " +
                    "" +
                    "CONSTRAINT pk_manager_id PRIMARY KEY(id), " +
                    "CONSTRAINT chk_manager_salary CHECK (salary > MONEY(0))," +
                    "CONSTRAINT chk_manager_first_name CHECK(LENGTH(first_name) > 2));";

            Log.info(this.getClass().getSimpleName() + " MentorDaoImpl()", PreparedStatement.class.getSimpleName(), "Creating preparedStatement");
            preparedStatement = connection.prepareStatement(ddlQuery);
            preparedStatement.execute();

        } catch (SQLException e) {
            Log.error(this.getClass().getSimpleName() + " MentorDaoImpl()", e.getStackTrace()[0].getClass().getSimpleName(), e.getMessage());
            e.printStackTrace();
        } finally {
            close(preparedStatement);
            close(connection);
        }
    }

    @Override
    public Optional<Mentor> save(Mentor mentor) {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Mentor savedMentor = null;

        try {
            Log.info(this.getClass().getSimpleName() + " save()", Connection.class.getSimpleName(), "Establishing connection");
            connection = getConnection();

            String createQuery = "INSERT INTO tb_mentors(" +
                    "last_name, first_name, phone_number, salary, date_created, dob, email) " +

                    "VALUES(?, ?, ?, MONEY(?), ?, ?, ?)";

            preparedStatement = connection.prepareStatement(createQuery);
            preparedStatement.setString(1, mentor.getLastName());
            preparedStatement.setString(2, mentor.getFirstName());
            preparedStatement.setString(3, mentor.getPhoneNumber());
            preparedStatement.setString(4, mentor.getSalary() + "");
            preparedStatement.setTimestamp(5, Timestamp.valueOf(mentor.getDateCreated()));
            preparedStatement.setDate(6, Date.valueOf(mentor.getDob()));
            preparedStatement.setString(7, mentor.getEmail());

            preparedStatement.execute();
            close(preparedStatement);

            String readQuery = "SELECT * FROM tb_mentors ORDER BY id DESC LIMIT 1";

            preparedStatement = connection.prepareStatement(readQuery);
            resultSet = preparedStatement.executeQuery();

            resultSet.next();

            savedMentor = new Mentor();
            savedMentor.setId(resultSet.getLong("id"));
            savedMentor.setFirstName(resultSet.getString("first_name"));
            savedMentor.setLastName(resultSet.getString("last_name"));
            savedMentor.setEmail(resultSet.getString("email"));
            savedMentor.setPhoneNumber(resultSet.getString("phone_number"));
            savedMentor.setSalary(Double.valueOf(resultSet.getString("salary").replaceAll("[^\\d\\.]", "")));
            savedMentor.setDob(resultSet.getDate("dob").toLocalDate());
            savedMentor.setDateCreated(resultSet.getTimestamp("date_created").toLocalDateTime());

        } catch (SQLException e) {
            Log.error(this.getClass().getSimpleName() + " MentorDaoImpl()", e.getStackTrace()[0].getClass().getSimpleName(), e.getMessage());
            e.printStackTrace();
        } finally {
            close(resultSet);
            close(preparedStatement);
            close(connection);
        }
        return Optional.of(savedMentor);
    }


    @Override
    public Optional<Mentor> findById(Long id) {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Mentor mentor = null;

        try {
            Log.info(this.getClass().getSimpleName() + " findById(" + id + ")", Connection.class.getSimpleName(), "Establishing connection");
            connection = getConnection();

            String readQuery = "SELECT * FROM tb_mentors WHERE id = ?";

            preparedStatement = connection.prepareStatement(readQuery);
            preparedStatement.setLong(1, id);

            resultSet = preparedStatement.executeQuery();
            resultSet.next();

            mentor = new Mentor();
            mentor.setId(resultSet.getLong("id"));
            mentor.setFirstName(resultSet.getString("first_name"));
            mentor.setLastName(resultSet.getString("last_name"));
            mentor.setEmail(resultSet.getString("email"));
            mentor.setPhoneNumber(resultSet.getString("phone_number"));
            mentor.setSalary(Double.valueOf(resultSet.getString("salary").replaceAll("[^\\d\\.]", "")));
            mentor.setDob(resultSet.getDate("dob").toLocalDate());
            mentor.setDateCreated(resultSet.getTimestamp("date_created").toLocalDateTime());


        } catch (SQLException e) {
            Log.error(this.getClass().getSimpleName(), e.getStackTrace()[0].getClass().getSimpleName(), e.getMessage());
            e.printStackTrace();
        } finally {
            close(resultSet);
            close(preparedStatement);
            close(connection);
        }
        return Optional.of(mentor);
    }

    @Override
    public List<Mentor> findAll() {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        List<Mentor> mentors = new ArrayList<>();

        try {
            Log.info(this.getClass().getSimpleName() + " findAll()", Connection.class.getSimpleName(), "Establishing connection");
            connection = getConnection();

            String readQuery = "SELECT * FROM tb_mentors;";

            preparedStatement = connection.prepareStatement(readQuery);

            resultSet = preparedStatement.executeQuery();

            for (int i = 0; i <= mentors.size() && resultSet.next(); i++) {
                Mentor mentor = new Mentor();
                mentor.setId(resultSet.getLong("id"));
                mentor.setFirstName(resultSet.getString("first_name"));
                mentor.setLastName(resultSet.getString("last_name"));
                mentor.setEmail(resultSet.getString("email"));
                mentor.setPhoneNumber(resultSet.getString("phone_number"));
                mentor.setSalary(Double.valueOf(resultSet.getString("salary").replaceAll("[^\\d\\.]", "")));
                mentor.setDob(resultSet.getDate("dob").toLocalDate());
                mentor.setDateCreated(resultSet.getTimestamp("date_created").toLocalDateTime());
                mentors.add(mentor);
            }
            return mentors;
        } catch (Exception e) {
            Log.error(this.getClass().getSimpleName(), e.getStackTrace()[0].getClassName(), e.getMessage());
            e.printStackTrace();
        } finally {
            close(resultSet);
            close(preparedStatement);
            close(connection);
        }
        return null;
    }

    @Override
    public List<Mentor> saveAll(List<Mentor> mentors) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Mentor> savedMentors = new ArrayList<>();

        try {
            Log.info(this.getClass().getSimpleName() + " saveAll()", Connection.class.getSimpleName(), "Establishing connection");
            connection = getConnection();

            String insertQuery = "INSERT INTO tb_mentors(" +
                    "last_name, first_name, phone_number, salary, date_created, dob, email) " +

                    "VALUES(?, ?, ?, MONEY(?), ?, ?, ?)";

            connection.setAutoCommit(false);

            for (int i = 0; i < mentors.size(); i++) {
                Mentor mentor = mentors.get(i);
                preparedStatement = connection.prepareStatement(insertQuery);

                preparedStatement.setString(1, mentor.getLastName());
                preparedStatement.setString(2, mentor.getFirstName());
                preparedStatement.setString(3, mentor.getPhoneNumber());
                preparedStatement.setString(4, (mentor.getSalary() + "").replace(".", ","));
                preparedStatement.setTimestamp(5, Timestamp.valueOf(mentor.getDateCreated()));
                preparedStatement.setDate(6, Date.valueOf(mentor.getDob()));
                preparedStatement.setString(7, mentor.getEmail());

                preparedStatement.addBatch();

                if (i % 20 == 0 || i == mentors.size() - 1) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }
            }
            close(preparedStatement);

            String readQuery = "SELECT * FROM tb_mentors ORDER BY id LIMIT " + mentors.size();

            preparedStatement = connection.prepareStatement(readQuery);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                Mentor mentor = new Mentor();
                mentor.setId(resultSet.getLong("Id"));
                mentor.setLastName(resultSet.getString("last_name"));
                mentor.setFirstName(resultSet.getString("first_name"));
                mentor.setPhoneNumber(resultSet.getString("phone_number"));
                mentor.setSalary(Double.valueOf(resultSet.getString("salary").replaceAll("[^\\d\\.]", "")));
                mentor.setDateCreated(resultSet.getTimestamp("date_created").toLocalDateTime());
                mentor.setDob(resultSet.getDate("dob").toLocalDate());
                mentor.setEmail(resultSet.getString("email"));

                savedMentors.add(mentor);
            }


        } catch (Exception e) {
            Log.error(this.getClass().getSimpleName(), e.getStackTrace()[0].getClassName(), e.getMessage());
            e.printStackTrace();
        } finally {
            close(resultSet);
            close(preparedStatement);
            close(connection);
        }

        return savedMentors;
    }
}
