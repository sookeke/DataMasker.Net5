
# Overview of Data Masking

If you've ever needed to pull down databases from a live environment to stage or even dev you'll need to think about masking any personal and business-sensitive information. Enterprises run the risk of breaching sensitive information when sharing data to the public or copying production data into non-production environments for the purposes of application development, testing, or data analysis. This tool was designed to help reduce this risk by irreversibly replacing the original sensitive data with fictitious data so that production data can be shared safely with non-production users.

Data masking (also known as data sanitization, protection, data replacement and data anonymization) is the process of replacing sensitive information copied from production databases to test non-production databases with realistic, but scrubbed, data based on masking rules. Data masking" means altering data from its original state to protect it. This process is ideal for virtually any situation when confidential or regulated data needs to be shared with non-production users.

Data masking enables organizations to generate realistic and fully functional data with similar characteristics as the original data to replace sensitive or confidential information while sharing the data with the public or interested partners.

##     The goal of Data Masking

The Goal of data masking is to maintain the same structure of data so that it will work in applications. This often requires shuffling and replacement algorithms that leaves data such as number and data intact.

 

##       What sort of data needs to be masked?

Personally-identifiable information (PII) is common to most data masking requirements. PII is any data that can be used to identify a living person, and includes such elements as name, date of birth, National Identification Number, address details, phone numbers or email addresses, disabilities, gender identity or sexual orientation, court orders, electronic wage slips, union affiliations, biometric and 'distinguishing feature' information, references to the serial number of devices such as laptops that are associated with, or assigned to, a person. Names, addresses, phone numbers, and credit card details are examples of data that require protection of the information content from inappropriate visibility. Live production database environments contain valuable and confidential data—access to this information is tightly controlled.

# Practical Challenges associated with Data Masking

###     Denormalization
For the quest of speeding up read-oriented data retrieval performance in a relational database, data designers stitch together disparate tables in the form of denormalization. If a database has been denormalized, the sensitive data will be stored in several tables and isn't always likely to be in an obvious place. The name of a customer, for example, will appear against addresses, phone numbers, invoice headers, correspondence, references, logs, transcriptions of conversations and so on. To mask even a simple customer name could be nearly impossible. Even a well-normalized database can accidentally reveal personal information if an XML or text field is stored.

In some cases, before changing a value there must exist several arcane rules that specify what else needs to be altered and where.

###     Constraints

For data integrity purpose, databases must have constraints, rules, functions and triggers that are there to ensure that data is consistent and reliable. In other words, they are there to restrict the very activity such as data masking, the direct alteration of data in the database tables.

A CHECK constraint can do basic checks on the value in a column but can also ensure that there is consistency at the table level. By altering one or more values, these CHECK constraint rules can be violated.
You can also run into problems if the column containing the value you are altering is participating in a PRIMARY KEY or FOREIGN KEY constraint. This can mean that many masking rules can only by executed in order, or in a particular way.
You can, of course, temporarily disable triggers, unique keys, check constraints or foreign key constraints while you perform the masking. This can be extremely useful, but the disabling of triggers can conceivably result in inconsistency, depending on the operation that the triggers needed to perform. Also, of course, you'll have to reenable the constraints and keys at some point, and this can be the time you realize that there are a lot of inconsistencies that need mending.

Faced with the difficulties of altering data within a database, you might think that a better approach is to apply in-passage masking to text versions of the base tables, using scripts and Regexes, or by creating a look up table that will provide an alias as a key valued pair.

###     Distributed databases
Another problem can happen if your extracted data set originates in more than one database or instance. The masking software tends to work only on a single database instance(multiple schemas), and you can get problems with masking the data within several databases in a way that yields consistent data.

###     Primary key as Sensitive

Data masking that is part of a primary key can be a huge challenge to the entire integrity of the database. If you attempt to alter the data in a column that participates in a PRIMARY KEY, then you'll likely destroy the referential integrity of the database. To do this effectively, using a substitution strategy in a database, you will need to create a correlation table, which contains copies of the before- and after- values of the column to be masked. The correlation table is then used to relate the masked, or substituted, key values to the original ones, making sure that the new values are unique.