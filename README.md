[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)](https://github.com/bcgov/pims/issues)
![API (.Net Core)](<https://github.com/bcgov/PSP/workflows/API%20(.NET%20Core)/badge.svg?branch=dev>)
[![codecov](https://codecov.io/gh/bcgov/PSP/branch/dev/graph/badge.svg)](https://codecov.io/gh/bcgov/PSP)
![Uptime Robot status](https://img.shields.io/uptimerobot/status/m784832378-1d844c019bc2900c17c826cb)
[![img](https://img.shields.io/badge/Lifecycle-Stable-97ca00)](https://github.com/bcgov/repomountie/blob/master/doc/lifecycle-badges.md)

# Data Masker
Data Masking Utility Tool .Net 5

# Introduction

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


<div>
   <div style="text-align: center;">&#160;</div>&#160;</div>
<div align="center">
   <table class="ms-rteTable-4" cellspacing="0" style="width: 80%; text-align: center;">
      <tbody>
         <tr class="ms-rteTableEvenRow-4">
            <td class="ms-rteTableEvenCol-4" rowspan="1" colspan="1" style="width: 33.33%;">
               <div> 
                  <span>&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;</span></div>
               <div> 
                  <span>&#160; 
                     <i class="fa fa-file-code"></i></span></div>
               <div> 
                  <span> </span>
                  <h2> 
                     <a href="https://github.com/sookeke/DataMasker.Net5/wiki">DevOps</a><br/></h2>
               </div>
               <p style="text-align: center;">
                  <span aria-hidden="true">Git, Git Action,&#160;SVN, Jenkins, JIRA<br/></span></p>
            </td>
            <td class="ms-rteTableOddCol-4" rowspan="1" colspan="1" style="width: 33.33%;">
               <div> 
                  <span> &#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;</span></div>
               <div> 
                  <span>
                     <i class="fa fa-drafting-compass"></i></span>&#160;</div>
               <div> 
                  <span> </span>
                  <h2>
                     <a href="https://github.com/sookeke/DataMasker.Net5/wiki/Data-Classification">Data Classification</a></h2>
               </div>
               <div style="text-align: center;"> 
                  <span><span> </span></span>
                  <p>
                     <span aria-hidden="true"><span aria-hidden="true"></span>BA, SME, SA, OCIO Compliance by Zone (A, B, C)<span aria-hidden="true"><br/></span></span></p>
               </div>
            </td>
            <td class="ms-rteTableEvenCol-4" rowspan="1" colspan="1" style="width: 33.33%;">
               <div> 
                  <span> &#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160; </span>
                  <div>
                     <span><i class="fa fa-sync"></i></span> &#160;</div>
                  <span></span>
                  <div> 
                     <span> </span>
                     <h2> 
                        <a href="/guidelines/data_masking/SitePages/Data%20Masking%20Lifecycle.aspx">Data Masking Lifecycl</a>e<br/></h2>
                     <div style="text-align: center;"> 
                        <span><span> </span></span>
                        <p>
                           <span aria-hidden="true"><span aria-hidden="true"></span>Data Generation, Data Masking, Applied Masking, Scrambling<br/></span></p>
                        <span> </span>
                        <p> 
                           <span aria-hidden="true">
                              <span aria-hidden="true">
                                 <br/></span></span></p>
                     </div>
                  </div>
               </div>
            </td>
         </tr>
      </tbody>
   </table> 
   <br/> 
</div>
<div>
   <div style="text-align: center;">&#160;</div>&#160;</div>
<div align="center">
   <table class="ms-rteTable-4" cellspacing="0" style="width: 80%; text-align: center;">
      <tbody>
         <tr class="ms-rteTableEvenRow-4">
            <td class="ms-rteTableEvenCol-4" rowspan="1" colspan="1" style="width: 33.33%;">
               <div> 
                  <span>&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;</span></div>
               <div> 
                  <span>&#160; 
                     <i class="fa fa-check"></i></span></div>
               <div> 
                  <span> </span>
                  <h2> 
                     <a href="https://github.com/sookeke/DataMasker.Net5/wiki/Validation-Check">Validation Check</a></h2>
               </div>
               <p style="text-align: center;">
                  <span aria-hidden="true">Data Validation, Email Delivery</span></p>
            </td>
            <td class="ms-rteTableOddCol-4" rowspan="1" colspan="1" style="width: 33.33%;">
               <div> 
                  <span> &#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;</span></div>
               <div> 
                  <span>
                     <i class="fa fa-server"></i></span>&#160;</div>
               <div> 
                  <span> </span>
                  <h2>
                     <a href="https://github.com/sookeke/DataMasker.Net5/wiki/Solution-Architecture">Solution Architecture</a><br/></h2>
               </div>
               <div style="text-align: center;"> 
                  <span><span> </span></span>
                  <p>
                     <span aria-hidden="true"><span aria-hidden="true"></span>Repository Pattern, Data Mapper<br/></span></p>
                  <span> </span>
                  <p> 
                     <span aria-hidden="true">
                        <span aria-hidden="true">
                           <br/></span></span></p>
               </div>
            </td>
            <td class="ms-rteTableEvenCol-4" rowspan="1" colspan="1" style="width: 33.33%;">
               <div> 
                  <span> &#160;&#160;&#160; &#160;</span>
                  <div style="text-align: center;">&#160;&#160;<span class="fa fa-file-code"></span></div>
                  <div style="text-align: center;">
                     <h2>
                        <a href="/guidelines/data_masking/SitePages/Configuration.aspx">Configuration</a><br/></h2>
                  </div>
                  <p style="text-align: center;">
                     <span aria-hidden="true">Git,&#160;SVN, Jenkins, JIRA</span></p>
                  <span> &#160; &#160; &#160; &#160; &#160; &#160;</span>
                  <div>
                     <div style="text-align: center;">
                     </div>
                  </div>
               </div>
            </td>
         </tr>
      </tbody>
   </table> 
   <br/> 
</div>
<div>
   <br/>
</div>
<div>
   <br/>
</div>
<div>
   <div class="ms-rtestate-read ms-rte-wpbox" contenteditable="false">
      <div class="ms-rtestate-notify  ms-rtestate-read 1c07bf2e-2426-4bb4-bc59-0d097567bca5" id="div_1c07bf2e-2426-4bb4-bc59-0d097567bca5">
      </div>
      <div id="vid_1c07bf2e-2426-4bb4-bc59-0d097567bca5" style="display: none;">
      </div>
   </div>
   <br/>
</div> 
<br/> 
<br/>
<br/>
<br/>
<br/>
