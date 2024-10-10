package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.*;


public class UserDaoJDBCImpl implements UserDao {
    private static final Logger logger = LoggerFactory.getLogger(UserDaoJDBCImpl.class);
    private final Util util;
    public UserDaoJDBCImpl() {
        this.util = new Util();
    }

    @Override
    public void createUsersTable() {
        String sql = "CREATE TABLE IF NOT EXISTS users (" +
                "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                "name VARCHAR(50), " +
                "lastName VARCHAR(50), " +
                "age TINYINT)";
        try (Connection connection = util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
            logger.info("Таблица 'users' успешно создана или уже существует.");
        } catch (SQLException e) {
            logger.error("Ошибка при создании таблицы 'users'.", e);
            throw new DaoException("Ошибка при создании таблицы 'users'.", e);
        }
    }

    @Override
    public void dropUsersTable() {
        String sql = "DROP TABLE IF EXISTS users";
        try (Connection connection = util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
            logger.info("Таблица 'users' успешно удалена или не существовала.");
        } catch (SQLException e) {
            logger.error("Ошибка при удалении таблицы 'users'.", e);
            throw new DaoException("Ошибка при удалении таблицы 'users'.", e);
        }
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        String sql = "INSERT INTO users (name, lastName, age) VALUES (?, ?, ?)";
        try (Connection connection = util.getConnection()) {
            connection.setAutoCommit(false); // Начало транзакции
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setString(1, name);
                preparedStatement.setString(2, lastName);
                preparedStatement.setByte(3, age);
                preparedStatement.executeUpdate();
                connection.commit();
                logger.info("User с именем – {} добавлен в базу данных.", name);
            } catch (SQLException e) {
                connection.rollback();
                logger.error("Ошибка при добавлении пользователя с именем – {}. Транзакция откатена.", name, e);
                throw new DaoException("Ошибка при добавлении пользователя.", e);
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            logger.error("Ошибка соединения с базой данных.", e);
            throw new DaoException("Ошибка соединения с базой данных.", e);
        }
    }

    @Override
    public void removeUserById(long id) {
        String sql = "DELETE FROM users WHERE id = ?";
        try (Connection connection = util.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setLong(1, id);
            int rowsAffected = preparedStatement.executeUpdate();
            if (rowsAffected > 0) {
                logger.info("User с id – {} удален из базы данных.", id);
            } else {
                logger.warn("User с id – {} не найден.", id);
            }
        } catch (SQLException e) {
            logger.error("Ошибка при удалении пользователя с id – {}.", id, e);
            throw new DaoException("Ошибка при удалении пользователя.", e);
        }
    }

    @Override
    public List<User> getAllUsers() {
        String sql = "SELECT * FROM users";
        List<User> users = new ArrayList<>();
        try (Connection connection = util.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("name"));
                user.setLastName(resultSet.getString("lastName"));
                user.setAge(resultSet.getByte("age"));
                users.add(user);
            }
            logger.info("Получен список всех пользователей.");
        } catch (SQLException e) {
            logger.error("Ошибка при получении списка пользователей.", e);
            throw new DaoException("Ошибка при получении списка пользователей.", e);
        }
        return users;
    }

    @Override
    public void cleanUsersTable() {
        String sql = "TRUNCATE TABLE users";
        try (Connection connection = util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
            logger.info("Таблица 'users' успешно очищена.");
        } catch (SQLException e) {
            logger.error("Ошибка при очистке таблицы 'users'.", e);
            throw new DaoException("Ошибка при очистке таблицы 'users'.", e);
        }
    }
}