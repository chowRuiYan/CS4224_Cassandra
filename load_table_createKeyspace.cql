-- Create cs4224 keyspace
CREATE  KEYSPACE IF NOT EXISTS cs4224_keyspace 
    WITH REPLICATION = { 
        'class' : 'SimpleStrategy',
        'replication_factor' : 1 
    };
