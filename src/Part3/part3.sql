-- 1) Написать функцию, возвращающую таблицу TransferredPoints в более человекочитаемом виде 

CREATE OR REPLACE FUNCTION get_transferred_points_human_readable()
    RETURNS TABLE
            (
                Peer1        VARCHAR,
                Peer2        VARCHAR,
                PointsAmount int
            )
AS
$$
begin
    return query
        SELECT checking_peer, checked_peer, points_amount
        FROM transferred_points
        EXCEPT
        SELECT t1.checking_peer, t1.checked_peer, t1.points_amount
        FROM transferred_points t1
                 JOIN transferred_points t2
                      ON t1.checking_peer = t2.checked_peer
        WHERE (t1.checking_peer, t1.checked_peer) = (t2.checked_peer, t2.checking_peer)
          AND t1.id <> t2.id

        UNION

        SELECT t1.checking_peer,
               t1.checked_peer,
               t1.points_amount - t2.points_amount AS points_amount
        FROM transferred_points t1
                 JOIN transferred_points t2
                      ON t1.checking_peer = t2.checked_peer
        WHERE (t1.checking_peer, t1.checked_peer) = (t2.checked_peer, t2.checking_peer)
          AND t1.id <> t2.id
          AND t1.checking_peer > t1.checked_peer
        ORDER BY 1, 2;
end;
$$
    LANGUAGE plpgsql;

--SELECT *
--from get_transferred_points_human_readable();


--2) Написать функцию, которая возвращает таблицу вида: ник пользователя, название проверенного задания, кол-во полученного XP

CREATE OR REPLACE FUNCTION get_xp_success_tasks()
    RETURNS TABLE
            (
                Peer VARCHAR,
                Task text,
                XP   integer
            )
AS
$$
begin
    return query
        SELECT c.peer, split_part(c.task, '_', 1) as Task, xp.xp_amount XP
        FROM checks c
                 join xp on c.id = xp.check
        order by 1, 2, 3 desc;
end;
$$ LANGUAGE plpgsql;


--SELECT *
--from get_xp_success_tasks();


--3) Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня
--В заданиях, относящихся к этой таблице, под действием "выходить" подразумеваются все покидания кампуса за день, кроме последнего.
--В течение одного дня должно быть одинаковое количество записей с состоянием 1 и состоянием 2 для каждого пира.

CREATE OR REPLACE FUNCTION get_peer_who_no_leaved_campus(date_check DATE)
    returns TABLE
            (
                Peer VARCHAR
            )
AS
$$
SELECT peer
from (SELECT peer, count(*) as c
      from time_tracking
      where date = date_check
        and state = 2
      GROUP by peer) tmp
where c = 1
order by 1
$$ LANGUAGE sql;

--SELECT *
--from get_peer_who_no_leaved_campus('2022-04-17');


--4) Найти процент успешных и неуспешных проверок за всё время

CREATE OR REPLACE PROCEDURE get_percent_success_failure_checks(ref refcursor) AS
$$
begin
    open ref for
        SELECT round(succ / total::NUMERIC * 100, 2)   AS SuccessfulChecks,
               round(unsucc / total::NUMERIC * 100, 2) AS UnsuccessfulChecks
        from (select (SELECT count(*) from p2p where state = 'Success') +
                     (SELECT count(*) from verter where state = 'Success') as succ,
                     (SELECT count(*) from p2p where state = 'Failure') +
                     (SELECT count(*) from verter where state = 'Failure') as unsucc,
                     (SELECT count(*) from p2p where state = 'Success') +
                     (SELECT count(*) from verter where state = 'Success') +
                     (SELECT count(*) from p2p where state = 'Failure') +
                     (SELECT count(*) from verter where state = 'Failure') as total) tmp;
end;
$$ LANGUAGE plpgsql;

--BEGIN;
--CALL get_percent_success_failure_checks('ref');
--FETCH ALL IN "ref";
--close ref;
--COMMIT;


--5) Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints

CREATE OR REPLACE PROCEDURE get_change_peer_points(ref refcursor) AS
$$
begin
    open ref for
        SELECT plus.peer,
               case
                   when (plus.peer is null) then minus.sum
                   when (minus.peer is null) then plus.sum
                   else plus.sum - minus.sum
                   end PointsChange
        from (SELECT checking_peer as peer, sum(points_amount)
              FROM transferred_points
              GROUP by 1) plus
                 full join
             (SELECT checked_peer as peer, sum(points_amount)
              FROM transferred_points
              GROUP by 1) minus on plus.peer = minus.peer
        ORDER by 2 desc, 1;
end;
$$ LANGUAGE plpgsql;

--BEGIN;
--CALL get_change_peer_points('ref');
--FETCH ALL IN "ref";
--close ref;
--COMMIT;


--6) Посчитать изменение в количестве пир поинтов каждого пира по таблице, возвращаемой первой функцией из Part 3

CREATE OR REPLACE PROCEDURE get_change_peer_points_from_func(ref refcursor) AS
$$
begin
    open ref for
        select peer, sum(pointsamount) as pointsamount
        from (SELECT case
                         when (t1.peer1 is null) then t2.peer2
                         else t1.peer1
                         end Peer,
                     case
                         when (t1.peer1 is null) then t2.pointsamount * (-1)
                         when (t2.peer2 is null) then t1.pointsamount
                         else t1.pointsamount - t2.pointsamount
                         end pointsamount

              from (SELECT * from get_transferred_points_human_readable()) t1
                       FULL join
                       (SELECT * from get_transferred_points_human_readable()) t2 on t1.peer1 = t2.peer2) tmp
        GROUP by 1
        order by 2 desc, 1;
end;
$$ LANGUAGE plpgsql;

--BEGIN;
--CALL get_change_peer_points_from_func('ref');
--FETCH ALL IN "ref";
--close ref;
--COMMIT;

--7) Определить самое часто проверяемое задание за каждый день

CREATE or REPLACE PROCEDURE get_most_checked_task(ref refcursor) AS
$$
BEGIN
    open ref for
        SELECT to_char(day, 'DD.MM.YYYY') as day,
               split_part(task, '_', 1)   as task
        from (select date as day, task, count(*), max(count(*)) over (PARTITION by date )
              from checks
              GROUP by 1, 2
              order by 1 desc, 2) t
        where t.count = t.max;
end;
$$ LANGUAGE plpgsql;


--BEGIN;
--CALL get_most_checked_task('ref');
--FETCH ALL IN "ref";
--close ref;
--COMMIT;


--8) Определить длительность последней P2P проверки (Последняя у которой самое позднее время 
-- окончания проверки)?

CREATE or REPLACE PROCEDURE get_duration_last_check(ref refcursor) AS
$$
BEGIN
    open ref FOR
        with tmp as
                 (SELECT state, time
                  from (SELECT c.id
                        from checks c
                                 join p2p p on c.id = p.check
                        order by date desc, time desc
                        LIMIT 1) t1
                           join (SELECT * from p2p) t2
                                on t1.id = t2.check)

        SELECT (
                       (SELECT time from tmp where state in ('Success', 'Failure'))
                       -
                       (SELECT time from tmp where state = 'Start'))::time as time;
end;
$$ LANGUAGE plpgsql;

--BEGIN;
--CALL get_duration_last_check('ref');
--FETCH ALL IN "ref";
--close ref;
--COMMIT;


--9) Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания

create or REPLACE PROCEDURe get_peer_finished_block(ref refcursor, in name VARCHAR) AS
$$
BEGIN
    open ref for
        with block as
                 (SELECT distinct title as title
                  from tasks
                  where title SIMILAR to (NAME || '[0-9]%')),

             tmp as (SELECT peer, COUNT(*) as count, max(date) as day
                     from (SELECT DISTINCT peer, task, date
                           from checks c
                                    join xp on c.id = xp.check
                                    JOIN block b on c.task = b.title) t1
                     GROUP by peer)

        SELECT peer,
               to_char(day, 'DD.MM.YYYY') as day
        from tmp
        where count = (SELECT COUNT(*) from block);
end;
$$ LANGUAGE plpgsql;

--BEGIN;
--call get_peer_finished_block('ref', 'CPP');
--FETCH ALL in "ref";
--close ref;
--end;

--10) Определить, к какому пиру стоит идти на проверку каждому обучающемуся

CREATE or REPLACE PROCEDURE get_recommended_peer(ref refcursor) AS
$$
BEGIN
    open ref for
        SELECT nickname                        as Peer,
               COALESCE(recommended_peer, '-') as RecommendedPeer
        from peers
                 left join
             (SELECT peer1, recommended_peer
              from (SELECT *, max(count) over (PARTITION by peer1) as max
                    FROM (SELECT peer1,
                                 recommended_peer,
                                 COUNT(*) as count
                          from (SELECT peer1, peer2
                                from friends
                                UNION ALL
                                SELECT peer2, peer1
                                from friends
                                order by 1) t1
                                   join
                                   (SELECT peer, recommended_peer from recommendations) t2
                                   on t1.peer2 = t2.peer
                          where peer1 <> recommended_peer
                          GROUP by 1, 2
                          order by 1, 2) tmp) t3
              where count = max) t4
             on peers.nickname = t4.peer1
        order by 1, 2;
end;
$$ LANGUAGE plpgsql;


--BEGIN;
--CALL get_recommended_peer('ref');
--FETCH ALL IN "ref";
--close ref;
--COMMIT;

--11) Определить процент пиров, которые:
--Приступили только к блоку 1
--Приступили только к блоку 2
--Приступили к обоим
--Не приступили ни к одному

create or REPLACE PROCEDURE get_percent_started_blocks(ref refcursor, in block1 VARCHAR, in block2 VARCHAR) AS
$$
BEGIN
    open ref for
        with first_block as (SELECT DISTINCT nickname
                             FROM peers p
                                      left join checks c on p.nickname = c.peer
                             where task SIMILAR to (block1 || '[0-9]%')),

             second_block as (SELECT DISTINCT nickname
                              FROM peers p
                                       left join checks c on p.nickname = c.peer
                              where task SIMILAR to (block2 || '[0-9]%')),

             nothing_block as (select nickname
                               FROM peers
                               EXCEPT
                               SELECT *
                               from FIRST_block
                               EXCEPT
                               SELECT *
                               from SECOND_block),

             both_block as (select *
                            from FIRST_block
                            INTERSECT
                            SELECT *
                            from SECOND_block),

             only_first as (select *
                            from FIRST_block
                            EXCEPT
                            SELECT*
                            from SECOND_block),

             ONLY_second as (select *
                             from SECOND_block
                             EXCEPT
                             SELECT*
                             from FIRST_block)

        SELECT case
                   when (SELECT COUNT(*) from peers) = 0 then 0
                   else round((SELECT count(*) from only_first)::numeric / (SELECT COUNT(*) from peers) * 100, 2)
                   end as StartedBlock1,
               case
                   when (SELECT COUNT(*) from peers) = 0 then 0
                   else round((SELECT count(*) from only_second)::numeric / (SELECT COUNT(*) from peers) * 100, 2)
                   end as StartedBlock2,
               case
                   when (SELECT COUNT(*) from peers) = 0 then 0
                   else round((SELECT count(*) from both_block)::numeric / (SELECT COUNT(*) from peers) * 100, 2)
                   end as StartedBothBlocks,
               case
                   when (SELECT COUNT(*) from peers) = 0 then 0
                   else round((SELECT count(*) from NOTHING_block)::numeric / (SELECT COUNT(*) from peers) * 100, 2)
                   end as DidntStartAnyBlock;
end;
$$ LANGUAGE plpgsql;

--begin;
--call get_percent_started_blocks('ref', 'C', 'A');
--fetch all in "ref";
--close ref;
--commit;


--12) Определить N пиров с наибольшим числом друзей

CREATE or REPLACE PROCEDURE get_peers_have_more_friends(ref refcursor, in n integer) as
$$
BEGIN
    open ref for
        SELECT nickname as peer, COALESCE(tmp.count, 0) as FriendsCount
        from peers p
                 LEFT join
             (SELECT t.peer, COUNT(*)
              from (SELECT peer1 as peer
                    from friends
                    UNION all
                    SELECT peer2
                    FROM friends) t
              GROUP by 1) tmp on p.nickname = tmp.peer
        order by 2 desc, 1
        limit n;
end;
$$ LANGUAGE plpgsql;

--begin;
--call get_peers_have_more_friends('ref', 3);
--FETCH all in "ref";
--close ref;
--COMMIT;

--13) Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения

CREATE or REPLACE PROCEDURE get_percent_peer_birthday_checked(ref refcursor) AS
$$
BEGIN
    open ref for
        with birthday_project as --id проектов проверенных в чей то ДР
                 (SELECT id, peer
                  from (SELECT nickname,
                               EXTRACT(month from birthday) as month,
                               EXTRACT(DAY from birthday)   as day
                        FROM peers) t1
                           join
                       (SELECT id,
                               peer,
                               EXTRACT(month from date) as month,
                               EXTRACT(DAY from date)   as day
                        from checks) t2
                       on (t1.nickname, t1.month, t1.day) = (t2.peer, t2.month, t2.day)),

             succ as --количество уникальных пиров прошедшие успешно проверки в ДР
                 (SELECT COUNT(*)
                  from (SELECT DISTINCT peer
                        from birthday_project bp
                                 join xp on bp.id = xp.check) t3),

             total as --общее количество уникальных пиров проходивших проверки в ДР
                 (SELECT COUNT(*)
                  from (SELECT DISTINCT peer from birthday_project) t4)

        SELECT case
                   when (SELECT * from total) = 0 then 0
                   else round((SELECT * from succ)::NUMERIC / (SELECT * from total) * 100, 2)
                   end as SuccessfulChecks,
               case
                   when (SELECT * from total) = 0 then 0
                   else round(((SELECT * from total) - (SELECT * from succ))::NUMERIC / (SELECT * from total) * 100, 2)
                   end as UnsuccessfulChecks;
end;
$$ LANGUAGE plpgsql;

--begin;
--call get_percent_peer_birthday_checked('ref');
--FETCH all in "ref";
--close ref;
--commit;

--25)

CREATE OR REPLACE PROCEDURE get_percentage_of_early_entries(ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
with AllEntries as (
SELECT m1 as Month, count(m2) c
from (
SELECT extract(month from p.birthday) as m1,
       extract(month from t.date) as m2 from peers p join time_tracking t on p.nickname = t.peer
where state =1) t1
where m1=m2
group by m1),

    EarlyEntries as (
SELECT m1 as Month, count(m2) c
from (
SELECT extract(month from p.birthday) as m1,
       extract(month from t.date) as m2 from peers p join time_tracking t on p.nickname = t.peer
where state =1 and t.time < '12:00:00') t1
where m1=m2
group by m1)

SELECT  t1.Month, coalesce(EarlyEntries, 0) from
(SELECT extract(month from gs) as gs, to_char(gs, 'Month') as Month
FROM (SELECT generate_series('2022-01-01', '2022-12-01', interval '1 month') as gs) tmp) t1
LEFT JOIN
(SELECT e.Month, round(e.c*100/(a.c::numeric), 2) as EarlyEntries from EarlyEntries e join AllEntries a on a.Month = e.Month) t2
on t1.gs = t2.Month;
END;
$$ LANGUAGE plpgsql;



BEGIN;
CALL get_percentage_of_early_entries('ref');
FETCH ALL IN "ref";
close ref;
END;
