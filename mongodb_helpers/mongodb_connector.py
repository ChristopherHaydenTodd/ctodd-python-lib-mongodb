#!/usr/bin/env python3
"""
    Purpose:
        Connector Library for MongoDB. Will provide a decorator for initiating a db_cur
        and injecting it as a parameter into a fuction.
"""

# Python Library Imports
import logging
import wrapt
import pymongo


def mongo_connector(
    hostname, username=None, password=None,
    port=27017, auth_method='SCRAM-SHA-1'):
    """
        Purpose:
            Decorator for connecting to mongodb database (mongos instance)
        Args:
            hostname (string): hostname of server
            username (string): username to connect to db with
            password (string): password of user
            port (int): port number for mongos instance
            auth_method (string): authentication method for mongodb
        Returns:
            decorator (function): function decorating another
                function and injecting a db_cur for conencting,
                committing, and closing the db connection
    """

    @wrapt.decorator
    def with_connection(f, instance, args, kwargs):
        """
            Purpose:
                Database conenction wrapping function
            Args:
                f (function/method): function being decorated
                instance: pass in self when wraping class method.
                    default is None when wraping function.
                args (Tuple): List of arguments
                kwargs (Dict): Dictionary of named arguments
            Return:
                cb_client (MongoDB Database Client): Client connected to
                    MongoDB Database (mongos instance)
            Function Termination:
                Will close connection to the database
        """

        print(
            'Connecting to MongoDB: {host}:{port}'.format(
                host=hostname, port=port
            )
        )

        db_client = pymongo.MongoClient(hostname, port)

        # Perform Auth Operation if Username is Provided
        if username:
            logging.info(
                'Authenticating with User {user} and Method {method}'.format(
                    user=username, method=auth_method
                )
            )
            db_client.admin.authenticate(
                username, password, mechanism=auth_method
            )

        try:
            output = f(db_client, *args, **kwargs)

        except Exception as err:
            logging.error(
                'Error Connecting to MongoDB: {0}'.format(err)
            )
            raise err

        finally:
            logging.info('Closing Database Cursor and Connection')
            db_client.close()

        return output

    return with_connection
