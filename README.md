**Description -**

The Campaign Management Mobile Device ID Match is an automation for producing real time data metrics that will enable
better decision making for the CM group in their mobile pixel id creation work.  The automation is deployed to run
twice a month, the 1st and the 15th, early am.
The automation begins by initiating an API call to the pixel builder server, this fetches a number of pixel ids
along will relevant information on their campaigns. The returned pixel ids are then used to find the corresponding
Jira tickets that they match up with.  The pixel numbers and campaign start dates are used to populate a hive query
which returns count results of MAIDS (hashed and un-hashed) matched to HHIDS and cookies as they all relate to the
pixel. The qubole calls are done concurrently, up to 10 at a time. The count results are used to calculate match
rates and all of the count and rate data is posted as a comment to the Jira ticket. The query count results are
saved as a json file on zfs1 on the Operations_mounted drive for further processing if desired by the CM team.

Additional functionality added: Includes a search for the parent (measurement) ticket of the pixel ticket. Upon 
successful search, add the same comment to the measurement ticket as the pixel ticket.  The reporter and lead analyst
are alerted via Jira comment post (if they exist on parent ticket). If no pixels are found to run, an alert email is
sent to the CM team.

**Application Information -**

Required modules: <ul>
                  <li>main.py,
                  <li>mobile_id_match_manager.py.py,
                  <li>pixel_name_search.py.py,
                  <li>jira_manager.py,
                  <li>qubole_manager.py,
                  <li>hhid_pixel_query.py,
                  <li>config.ini
                  </ul>

Location:         <ul>
                  <li>Deployment -> 
                  <li>Scheduled to run twice a month, triggered by ActiveBatch-V11 under File/Plan -> 
                  </ul>

Source Code:      <ul>
                  <li>
                  </ul>

LogFile Location: <ul>
                  <li>
                  </ul>

**Contact Information -**

Primary Users:    <ul>
                  <li>
                  </ul>

Lead Customer:    <ul>
                  <li>
                  </ul>

Lead Developer:   <ul>
                  <li>Bradley Ruck
                  </ul>

Date Launched:    <ul>
                  <li>June, 2018
                  </ul>
                  
Date Updated:     <ul>
                  <li>April, 2019
                  </ul>
