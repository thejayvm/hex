package config;

import controllers.PeopleController;
import hex.action.RouteManager;

/**
 * Created by jason on 14-11-16.
 */
public class ApplicationRoutes extends RouteManager {
    @Override
    public void defineRoutes() {
        get("/", PeopleController::new, "home");
        get("/people", PeopleController::new, "index");
        get("/people/:id", PeopleController::new, "show");
    }
}