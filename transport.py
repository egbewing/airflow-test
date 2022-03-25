# todo add import statements

class PostgresConnector():

    def __init__(self,
                 airflow_conn_id: str):
        self.airflow_conn_id = airflow_conn_id
        self.conn = None

    def __enter__(self):
        """
        this method should return a connection to a db
        :return:
        """
        print("----enter method----")
        self.conn = None
        return self.conn
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        this method should close the connection
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        print("----exit method-----")
        print("exiting this connector")
        pass


if __name__ == "__main__":
    with PostgresConnector(airflow_conn_id=1234) as thing:
        print(thing)
    print("out of with loop")
