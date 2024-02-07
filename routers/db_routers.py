


class AuthRouter:
    """
    A router to control all database operations on models in the
    auth and contenttypes applications.
    """
    route_app_labels = {'auth', 'contenttypes','admin','sessions'}

    def db_for_read(self, model, **hints):
        """
        Attempts to read auth and contenttypes models go to auth_db.
        """
        if model._meta.app_label in self.route_app_labels:
            return 'auth_db'
        return None

    def db_for_write(self, model, **hints):
        """
        Attempts to write auth and contenttypes models go to auth_db.
        """
        if model._meta.app_label in self.route_app_labels:
            return 'auth_db'
        return None

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations if a model in the auth or contenttypes apps is
        involved.
        """
        if (
            obj1._meta.app_label in self.route_app_labels or
            obj2._meta.app_label in self.route_app_labels
        ):
           return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Make sure the auth and contenttypes apps only appear in the
        'auth_db' database.
        """
        if app_label in self.route_app_labels:
            return db == 'auth_db'
        return None



class mssqlRouter:
 
    route_app_labels = {'portal'}

    def db_for_read(self, model, **hints):
  
        if model._meta.app_label in self.route_app_labels:
            return 'default'
        return None

    def db_for_write(self, model, **hints):
  
        if model._meta.app_label in self.route_app_labels:
            return 'default'
        return None

    def allow_relation(self, obj1, obj2, **hints):
 
        if (
            obj1._meta.app_label in self.route_app_labels or
            obj2._meta.app_label in self.route_app_labels
        ):
           return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):

        if app_label in self.route_app_labels:
            return db == 'default'
        return None
    
    
    
    # routers.py

class AuthDBRouter:
    """
    A database router to direct specific apps to the 'auth_db' database.
    """

    def db_for_read(self, model, **hints):
        """
        Choose the 'auth_db' database for read operations.
        """
        if model._meta.app_label == 'app_name':  # Replace 'app_name' with the actual app name.
            return 'auth_db'
        return None

    def db_for_write(self, model, **hints):
        """
        Choose the 'auth_db' database for write operations.
        """
        if model._meta.app_label == 'app_name':  # Replace 'app_name' with the actual app name.
            return 'auth_db'
        return None

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations if both objects are in the 'auth_db' database.
        """
        if (
            obj1._state.db == 'auth_db' and
            obj2._state.db == 'auth_db'
        ):
            return True
        return None
    

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Make sure the 'auth_db' database is used for specified apps.
        """
        if app_label == 'app_name':  # Replace 'app_name' with the actual app name.
            return db == 'auth_db'
        return None
