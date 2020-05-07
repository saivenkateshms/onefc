id event_name people_count
1  cricket  100
2  tennis   150
3  Rugby    300
4  badmiton 85
5  hockey   185
6  volley   190


select s.*
from (select s.*, count(*) over (partition by event_name) as cnt
      from (select s.*,
                   sum(case when people_count <= 100 then 1 else 0 end) over (order by event_name) as grp
            from stadium s
           ) s
     ) s
where cnt >= 3;
