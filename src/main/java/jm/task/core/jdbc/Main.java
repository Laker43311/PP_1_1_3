package jm.task.core.jdbc;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.service.UserService;
import jm.task.core.jdbc.service.UserServiceImpl;

public class Main {

    public static void main(String[] args) {
        UserService userService = new UserServiceImpl();

        userService.createUsersTable();

        userService.saveUser("Иван", "Иванов", (byte) 25);
        userService.saveUser("Мария", "Петрова", (byte) 30);
        userService.saveUser("Алексей", "Сидоров", (byte) 35);
        userService.saveUser("Елена", "Кузнецова", (byte) 28);

        for (User user : userService.getAllUsers()) {
            System.out.println(user);
        }

        userService.cleanUsersTable();
        userService.dropUsersTable();
    }
}
