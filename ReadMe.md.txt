- Ready Credential and Connectting to another SQL server.
 If you want to migrate the data to another SQL server  you must get the credential for the server..
 Then you must edit the io_config.yaml file  with the information.
If you complete this step, you can access another SQL server now.
- Getting table structure by change value of veriable in loader and Running loader
 And you must get the table structure according to text file.
 To do this, you can chage the value of "owner_name"  that you want to migrate owner of tables in load_oracle loader.
 If you set this and run this loader, then you will get the structures of orqacle database table contain table_name and data_type.
-Changing path of input file and Running exporter
 Then  you must change the value of file_name and file_path in oracle_data_exporter.
 If you set this values in correctly, you can migrate the data by running the exporter.