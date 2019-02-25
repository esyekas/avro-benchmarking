# Avro Benchmarking
Performance testing of the simple type and complex type of avro object by serlization and de-serilization

Schema format using String type:
{
 "namespace": "example.avro",
 "type": "record",
 "name": "Message",
 "fields": [
     {"name": "header", "type": "string"},
     {"name": "payload",  "type": "string"}
 ]
}

Schema format using map type:

{
 "namespace": "example.avro",
 "type": "record",
 "name": "Message_map",
 "fields": [
     {
        "name": "map",
         "type":
            {   "type": "map",
                "values":"string"
            }
     }
 ]
}


## Result:
  Benchamrk Summary - String type [825ms]
  {
    #tests: 1000
    #repetitions: 1


    64 bytes payload:
         mean_time[μs]: 151.84833899999873
      size_of_msg[bytes]: 4.0
  }
  
  Benchamrk Summary - Map type [777ms]
  {
    #tests: 1000
    #repetitions: 1

    64 bytes payload:
         mean_time[μs]: 141.17873400000133
      size_of_msg[bytes]: 6.0
  }
  


## Conclusion:
Simple type (String) takes less bytes and less processing time to seriliaze and deserialize it as compared to comples type(Map) in avro.



