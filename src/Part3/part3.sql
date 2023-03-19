-------------------------------------< 01 >-------------------------------------
-- Функция, возвращающая таблицу TransferredPoints в человекочитаемом виде.
-- Формат вывода: ник пира 1, ник пира 2, количество переданных пир поинтов.

CREATE OR REPLACE FUNCTION func_get_transferred_points_human_readable()
    RETURNS TABLE
            (
                Peer1        VARCHAR,
                Peer2        VARCHAR,
                PointsAmount int
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH tmp AS (SELECT DISTINCT t1.id,
                                     t1.checking_peer checking,
                                     t1.checked_peer  checked,
                                     t1.points_amount points

                     FROM transferred_points t1
                              JOIN transferred_points t2
                                   ON t1.checking_peer = t2.checked_peer
                     WHERE (t1.checking_peer, t1.checked_peer) =
                           (t2.checked_peer, t2.checking_peer)),
             result AS (SELECT checking_peer, checked_peer, points_amount
                        FROM transferred_points
                        EXCEPT ALL
                        SELECT checking, checked, points
                        FROM tmp

                        UNION ALL

                        SELECT t1.checking,
                               t1.checked,
                               points1 - points2 AS points
                        FROM (SELECT checking, checked, SUM(points) points1
                              FROM tmp
                              GROUP BY 1, 2) t1
                                 JOIN
                             (SELECT checking, checked, SUM(points) points2
                              FROM tmp
                              GROUP BY 1, 2) t2 ON (t1.checking, t1.checked) =
                                                   (t2.checked, t2.checking)
                        WHERE t1.checking > t1.checked)

        SELECT checking_peer AS        Peer1,
               checked_peer  AS        Peer2,
               SUM(points_amount)::int PointsAmount
        FROM result
        GROUP BY 1, 2
        ORDER BY 1, 2;
END;
$$
    LANGUAGE plpgsql;

-- SELECT * FROM func_get_transferred_points_human_readable();

-------------------------------------< 02 >-------------------------------------
-- Функция, которая возвращает таблицу вида: ник пользователя, название
-- проверенного задания, кол-во полученного XP.

CREATE OR REPLACE FUNCTION func_get_xp_success_tasks()
    RETURNS TABLE
            (
                Peer VARCHAR,
                Task text,
                XP integer
            )
AS
$$
BEGIN
    RETURN QUERY SELECT c.peer, SPLIT_PART(c.task, '_', 1) AS task,
                        xp.xp_amount AS xp
                 FROM checks c
                          JOIN xp
                               ON c.id = xp.check
                 ORDER BY 1, 2, 3 DESC;
END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM func_get_xp_success_tasks();

-------------------------------------< 03 >-------------------------------------
-- Функция, определяющая пиров, которые не выходили из кампуса в течение дня
-- (все покидания кампуса за день, кроме последнего)
-- Параметры функции: день. Функция возвращает только список пиров.

CREATE OR REPLACE FUNCTION func_get_peer_who_no_leaved_campus(date_check DATE)
    RETURNS TABLE
            (
                Peer VARCHAR
            )
AS
$$
SELECT peer
FROM (SELECT peer, COUNT(*) AS c
      FROM time_tracking
      WHERE date = date_check
        AND state = 2
      GROUP BY peer) tmp
WHERE c = 1
ORDER BY 1
$$ LANGUAGE sql;

-- SELECT * FROM func_get_peer_who_no_leaved_campus('2022-04-17');

-------------------------------------< 04 >-------------------------------------
-- Найти процент успешных и неуспешных проверок за всё время.
-- Формат вывода: процент успешных, процент неуспешных.

CREATE OR REPLACE PROCEDURE get_percent_success_failure_checks(ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT ROUND(succ / total::NUMERIC * 100, 2)   AS SuccessfulChecks,
               ROUND(unsucc / total::NUMERIC * 100, 2) AS UnsuccessfulChecks
        FROM (SELECT (SELECT COUNT(*) FROM p2p WHERE state = 'Success') +
                     (SELECT COUNT(*) FROM verter WHERE state = 'Success') AS succ,
                     (SELECT COUNT(*) FROM p2p WHERE state = 'Failure') +
                     (SELECT COUNT(*) FROM verter WHERE state = 'Failure') AS unsucc,
                     (SELECT COUNT(*) FROM p2p WHERE state = 'Success') +
                     (SELECT COUNT(*) FROM verter WHERE state = 'Success') +
                     (SELECT COUNT(*) FROM p2p WHERE state = 'Failure') +
                     (SELECT COUNT(*) FROM verter WHERE state = 'Failure') AS total) tmp;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_percent_success_failure_checks('ref');
-- FETCH ALL IN "ref";
-- CLOSE ref;
-- COMMIT;

-------------------------------------< 05 >-------------------------------------
-- Посчитать изменение в количестве пир поинтов каждого пира по TransferredPoints.
-- Формат вывода: ник пира, изменение в количество пир поинтов.
-- Сортировка по изменению числа поинтов.

CREATE OR REPLACE PROCEDURE get_change_peer_points(ref refcursor) AS
$$
BEGIN
    OPEN ref FOR SELECT plus.peer,
                        CASE
                            WHEN (plus.peer IS NULL) THEN minus.sum * (-1)
                            WHEN (minus.peer IS NULL) THEN plus.sum
                            ELSE plus.sum - minus.sum END AS pointschange
                 FROM (SELECT checking_peer AS peer, SUM(points_amount)
                       FROM transferred_points
                       GROUP BY 1) plus
                          FULL JOIN (SELECT checked_peer AS peer, SUM(points_amount)
                                     FROM transferred_points
                                     GROUP BY 1) minus
                                    ON plus.peer = minus.peer
                 ORDER BY 2 DESC, 1;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_change_peer_points('ref');
-- FETCH ALL IN "ref";
-- CLOSE ref;
-- COMMIT;

-------------------------------------< 06 >-------------------------------------
-- Посчитать изменение в количестве пир поинтов каждого пира по таблице из 01
-- Формат вывода: ник пира, изменение в количество пир поинтов.
-- Сортировка по изменению числа поинтов.

CREATE OR REPLACE PROCEDURE get_change_peer_points_from_func(ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT COALESCE(Peer1, Peer2) AS Peer,
               CASE
                   WHEN (t1.sum IS NULL) THEN t2.sum * (-1)
                   WHEN (t2.sum IS NULL) THEN t1.sum
                   ELSE t1.sum - t2.sum
                   END                AS PointsChange
        FROM (SELECT Peer1, SUM(pointsamount) sum
              FROM func_get_transferred_points_human_readable()
              GROUP BY Peer1) t1
                 FULL JOIN
             (SELECT Peer2, SUM(pointsamount) sum
              FROM func_get_transferred_points_human_readable()
              GROUP BY Peer2) t2
             ON t1.peer1 = t2.peer2
        ORDER BY 2 DESC, 1;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_change_peer_points_from_func('ref');
-- FETCH ALL IN "ref";
-- CLOSE ref;
-- COMMIT;

-------------------------------------< 07 >-------------------------------------
-- Определить самое часто проверяемое задание за каждый день.
-- Формат вывода: день, название задания.

CREATE OR REPLACE PROCEDURE get_most_checked_task(ref refcursor) AS
$$
BEGIN
    OPEN ref FOR SELECT TO_CHAR(day, 'DD.MM.YYYY') AS day,
                        SPLIT_PART(task, '_', 1) AS task
                 FROM (SELECT date AS day, task, COUNT(*),
                              MAX(COUNT(*)) OVER (PARTITION BY date)
                       FROM checks
                       GROUP BY 1, 2
                       ORDER BY 1 DESC, 2) t
                 WHERE t.count = t.max;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_most_checked_task('ref');
-- FETCH ALL IN "ref";
-- CLOSE ref;
-- COMMIT;

-------------------------------------< 08 >-------------------------------------
-- Определить длительность последней P2P проверки (разница между "началом"
-- и статусом "успех" или "неуспех"). Формат вывода: длительность проверки.

CREATE OR REPLACE PROCEDURE get_duration_last_check(ref refcursor) AS
$$
BEGIN
    OPEN ref FOR WITH tmp AS (SELECT state, time
                              FROM (SELECT c.id
                                    FROM checks c
                                             JOIN p2p p
                                                  ON c.id = p.check
                                    ORDER BY date DESC, time DESC
                                    LIMIT 1) t1
                                       JOIN (SELECT * FROM p2p) t2
                                            ON t1.id = t2.check)

                 SELECT ((SELECT time FROM tmp WHERE state IN ('Success', 'Failure'))
                     -
                         (SELECT time FROM tmp WHERE state = 'Start'))::time AS time;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_duration_last_check('ref');
-- FETCH ALL IN "ref";
-- CLOSE ref;
-- COMMIT;

-------------------------------------< 09 >-------------------------------------
-- Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания.
-- Параметры процедуры: название блока, например "CPP".
-- Формат вывода: ник пира, дата завершения блока.

CREATE OR REPLACE PROCEDURE get_peer_finished_block(ref refcursor, IN name VARCHAR) AS
$$
BEGIN
    OPEN ref FOR
        WITH block AS (SELECT DISTINCT title AS title
                       FROM tasks
                       WHERE title SIMILAR TO (name || '[0-9]%')),

             tmp AS (SELECT peer, COUNT(*) AS count, MAX(date) AS day
                     FROM (SELECT DISTINCT peer, task, date
                           FROM checks c
                                    JOIN xp
                                         ON c.id = xp.check
                                    JOIN block b
                                         ON c.task = b.title) t1
                     GROUP BY peer)

        SELECT peer, TO_CHAR(day, 'DD.MM.YYYY') AS day
        FROM tmp
        WHERE count = (SELECT COUNT(*) FROM block);
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_peer_finished_block('ref', 'C');
-- FETCH ALL IN "ref";
-- CLOSE ref;
-- END;

-------------------------------------< 10 >-------------------------------------
-- Определить, к какому пиру стоит идти на проверку каждому обучающемуся
-- (нужно найти пира, проверяться у которого рекомендует наибольшее число друзей).
-- Формат вывода: ник пира, ник найденного проверяющего.

CREATE or REPLACE PROCEDURE get_recommended_peer(ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT nickname                        AS Peer,
               COALESCE(recommended_peer, '-') AS RecommendedPeer
        FROM peers
                 LEFT JOIN
             (SELECT peer1, recommended_peer
              FROM (SELECT *, MAX(count) OVER (PARTITION BY peer1) AS max
                    FROM (SELECT peer1,
                                 recommended_peer,
                                 COUNT(*) AS count
                          from (SELECT peer1, peer2
                                FROM friends
                                UNION ALL
                                SELECT peer2, peer1
                                FROM friends
                                ORDER BY 1) t1
                                   JOIN
                               (SELECT peer, recommended_peer FROM recommendations) t2
                               ON t1.peer2 = t2.peer
                          WHERE peer1 <> recommended_peer
                          GROUP BY 1, 2
                          ORDER BY 1, 2) tmp) t3
              WHERE count = max) t4
             ON peers.nickname = t4.peer1
        ORDER BY 1, 2;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_recommended_peer('ref');
-- FETCH ALL IN "ref";
-- CLOSE ref;
-- COMMIT;

-------------------------------------< 11 >-------------------------------------
-- Определить процент пиров, которые: приступили только к блоку 1,
-- приступили только к блоку 2, приступили к обоим, не приступили ни к одному.

-- Формат вывода: % приступивших только к 1 блоку, % приступивших только ко 2 блоку,
-- % приступивших к обоим, % не приступивших ни к одному.

CREATE OR REPLACE PROCEDURE get_percent_started_blocks(ref refcursor,
                                                       IN block1 VARCHAR,
                                                       IN block2 VARCHAR) AS
$$
BEGIN
    OPEN ref FOR WITH first_block AS (SELECT DISTINCT nickname
                                      FROM peers p
                                               LEFT JOIN checks c
                                                         ON p.nickname = c.peer
                                      WHERE task SIMILAR TO (block1 || '[0-9]%')),

                      second_block AS (SELECT DISTINCT nickname
                                       FROM peers p
                                                LEFT JOIN checks c
                                                          ON p.nickname = c.peer
                                       WHERE task SIMILAR TO (block2 || '[0-9]%')),

                      nothing_block AS (SELECT nickname
                                        FROM peers
                                        EXCEPT
                                        SELECT *
                                        FROM first_block
                                        EXCEPT
                                        SELECT *
                                        FROM second_block),

                      both_block AS (SELECT *
                                     FROM first_block
                                     INTERSECT
                                     SELECT *
                                     FROM second_block),

                      only_first AS (SELECT *
                                     FROM first_block
                                     EXCEPT
                                     SELECT*
                                     FROM second_block),

                      only_second AS (SELECT *
                                      FROM second_block
                                      EXCEPT
                                      SELECT*
                                      FROM first_block)

                 SELECT CASE WHEN (SELECT COUNT(*) FROM peers) = 0 THEN 0
                             ELSE ROUND(
                                             (SELECT COUNT(*) FROM only_first)::numeric /
                                             (SELECT COUNT(*) FROM peers) *
                                             100, 2) END AS startedblock1,
                        CASE WHEN (SELECT COUNT(*) FROM peers) = 0 THEN 0
                             ELSE ROUND(
                                             (SELECT COUNT(*) FROM only_second)::numeric /
                                             (SELECT COUNT(*) FROM peers) *
                                             100, 2) END AS startedblock2,
                        CASE WHEN (SELECT COUNT(*) FROM peers) = 0 THEN 0
                             ELSE ROUND(
                                             (SELECT COUNT(*) FROM both_block)::numeric /
                                             (SELECT COUNT(*) FROM peers) *
                                             100, 2) END AS startedbothblocks,
                        CASE WHEN (SELECT COUNT(*) FROM peers) = 0 THEN 0
                             ELSE ROUND(
                                             (SELECT COUNT(*) FROM nothing_block)::numeric /
                                             (SELECT COUNT(*) FROM peers) *
                                             100,
                                             2) END AS didntstartanyblock;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_percent_started_blocks('ref', 'C', 'DO');
-- FETCH ALL IN "ref";
-- CLOSE ref;
-- COMMIT;

-------------------------------------< 12 >-------------------------------------
-- Определить N пиров с наибольшим числом друзей.
-- Формат вывода: ник пира, кол-во друзей, сортировка по кол-ву друзей.
-- Параметры процедуры: количество пиров N.

CREATE OR REPLACE PROCEDURE get_peers_have_more_friends(ref refcursor, IN n integer) AS
$$
BEGIN
    OPEN ref FOR SELECT nickname AS peer, COALESCE(tmp.count, 0) AS friendscount
                 FROM peers p
                          LEFT JOIN (SELECT t.peer, COUNT(*)
                                     FROM (SELECT peer1 AS peer
                                           FROM friends
                                           UNION ALL
                                           SELECT peer2
                                           FROM friends) t
                                     GROUP BY 1) tmp
                                    ON p.nickname = tmp.peer
                 ORDER BY 2 DESC, 1
                 LIMIT n;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_peers_have_more_friends('ref', 3);
-- FETCH ALL IN "ref";
-- CLOSE ref;
-- COMMIT;

-------------------------------------< 13 >-------------------------------------
-- Определить % пиров, которые когда-либо успешно/неуспешно проходили проверку
-- в свой день рождения. Формат вывода: % успехов в др, % неуспехов в др.

CREATE OR REPLACE PROCEDURE get_percent_peer_birthday_checked(ref refcursor) AS
$$
BEGIN
    OPEN ref FOR WITH birthday_project AS --id проектов проверенных в чей-то ДР
                          (SELECT id, peer
                           FROM (SELECT nickname,
                                        EXTRACT(MONTH FROM birthday) AS month,
                                        EXTRACT(DAY FROM birthday) AS day
                                 FROM peers) t1
                                    JOIN (SELECT id, peer,
                                                 EXTRACT(MONTH FROM date) AS month,
                                                 EXTRACT(DAY FROM date) AS day
                                          FROM checks) t2
                                         ON (t1.nickname, t1.month, t1.day) =
                                            (t2.peer, t2.month, t2.day)),

                      succ AS --количество уникальных пиров, прошедших успешно проверки в ДР
                          (SELECT COUNT(*)
                           FROM (SELECT DISTINCT peer
                                 FROM birthday_project bp
                                          JOIN xp
                                               ON bp.id = xp.check) t3),

                      total AS --общее количество уникальных пиров, проходивших проверки в ДР
                          (SELECT COUNT(*)
                           FROM (SELECT DISTINCT peer FROM birthday_project) t4)

                 SELECT CASE WHEN (SELECT * FROM total) = 0 THEN 0
                             ELSE ROUND((SELECT * FROM succ)::NUMERIC /
                                        (SELECT * FROM total) * 100,
                                        2) END AS successfulchecks,
                        CASE WHEN (SELECT * FROM total) = 0 THEN 0
                             ELSE ROUND(
                                             ((SELECT * FROM total) - (SELECT * FROM succ))::NUMERIC /
                                             (SELECT * FROM total) * 100,
                                             2) END AS unsuccessfulchecks;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_percent_peer_birthday_checked('ref');
-- FETCH ALL IN "ref";
-- CLOSE ref;
-- COMMIT;

-------------------------------------< 14 >-------------------------------------
-- Определить кол-во XP, полученное в сумме каждым пиром.
-- Формат вывода: ник пира, количество XP, сортировка по XP.

CREATE OR REPLACE PROCEDURE get_total_amount_xp_by_peer(ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        WITH best_xp AS (SELECT checks.peer, MAX(xp_amount) AS max
                         FROM checks
                                  JOIN xp ON checks.id = xp."check"
                         GROUP BY checks.peer, checks.task)
        SELECT best_xp.peer, SUM(best_xp.max) AS xp
        FROM best_xp
        GROUP BY best_xp.peer
        ORDER BY xp DESC;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_total_amount_xp_by_peer('ref');
-- FETCH ALL IN "ref";
-- END;

-------------------------------------< 15 >-------------------------------------
-- Определить пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3.
-- Параметры процедуры: названия заданий 1, 2 и 3; формат вывода: список пиров.

CREATE OR REPLACE PROCEDURE get_done_tasks_1_and_2_but_not_3(IN task1 varchar,
                                                             IN task2 varchar,
                                                             IN task3 varchar,
                                                             ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT nickname
        FROM (SELECT p.nickname,
                     CASE
                         WHEN (task LIKE task1 || '%') THEN 1
                         WHEN (task LIKE task2 || '%') THEN 1
                         WHEN (p.nickname IN (SELECT DISTINCT nickname
                                              FROM peers p
                                                       JOIN
                                                   (SELECT DISTINCT peer, task
                                                    FROM peers p
                                                             JOIN checks c ON p.nickname = c.peer) t1
                                                   ON nickname = t1.peer
                                              WHERE task LIKE task3 || '%'))
                             THEN 1
                         END AS task
              FROM peers p
                       JOIN checks c ON p.nickname = c.peer) tmp
        GROUP BY nickname
        HAVING SUM(task) = 2;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_done_tasks_1_and_2_but_not_3('C2', 'C3', 'C5', 'ref');
-- FETCH ALL IN "ref";
-- END;

-------------------------------------< 16 >-------------------------------------
-- Вывести кол-во предшествующих задач для каждой задачи, используя рекурсивное
-- обобщенное табличное выражение.
-- Формат вывода: название задачи, количество предшествующих.

CREATE OR REPLACE PROCEDURE get_number_of_preceding_tasks(ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        WITH RECURSIVE rec AS
                           (SELECT (CASE
                                        WHEN parent_task IS NULL THEN 0
                                        ELSE 1 END) AS count,
                                   title,
                                   parent_task      AS current_task,
                                   parent_task
                            FROM tasks
                            UNION ALL
                            SELECT (CASE
                                        WHEN next.parent_task IS NOT NULL
                                            THEN count + 1
                                        ELSE count END) AS count,
                                   next.title,
                                   next.parent_task     AS current_task,
                                   prev.title           AS parrent_task
                            FROM tasks AS next
                                     CROSS JOIN rec AS prev
                            WHERE prev.title LIKE next.parent_task)

        SELECT SPLIT_PART(title, '_', 1)::varchar AS Task,
               MAX(count)                         AS PrevCount
        FROM rec
        GROUP BY Task
        ORDER BY Task;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_number_of_preceding_tasks('ref');
-- FETCH ALL IN "ref";
-- END;

-------------------------------------< 17 >-------------------------------------
-- Найти "удачные" для проверок дни. День считается "удачным", если в нем есть
-- хотя бы N идущих подряд успешных проверок (подразумеваются успешные проверки,
-- между которыми нет неуспешных, количество опыта >= 80% от максимального).

-- Параметры процедуры: количество идущих подряд успешных проверок N.
-- Формат вывода: список дней.

CREATE OR REPLACE PROCEDURE get_lucky_days_for_checks(IN N integer, IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR WITH succesful_cheks AS (SELECT checks.id,
                                                 checks.peer,
                                                 checks.date,
                                                 verter."check" AS id_check,
                                                 tasks.max_xp,
                                                 xp.xp_amount,
                                                 verter.state
                                          FROM checks
                                                   JOIN p2p ON checks.id = p2p."check"
                                                   LEFT JOIN verter ON checks.id = verter."check"
                                                   JOIN tasks ON checks.task = tasks.title
                                                   JOIN xp ON checks.id = xp."check"
                                          WHERE p2p.state = 'Success'
                                            AND (verter.state = 'Success' OR verter.state IS NULL))
                 SELECT date
                 FROM succesful_cheks
                 WHERE succesful_cheks.xp_amount >= succesful_cheks.max_xp * 0.8
                 GROUP BY date
                 HAVING COUNT(date) >= N;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_lucky_days_for_checks(2,'ref');
-- FETCH ALL IN "ref";
-- END;

-------------------------------------< 18 >-------------------------------------
-- Определить пира с наибольшим числом выполненных заданий.
-- Формат вывода: ник пира, число выполненных заданий.

CREATE OR REPLACE PROCEDURE get_peer_max_completed_tasks(ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT checks.peer, COUNT(*) AS xp
        FROM checks
                 LEFT JOIN verter ON checks.id = verter."check"
                 JOIN p2p ON checks.id = p2p."check"
        WHERE p2p.state = 'Success'
          AND (verter.state = 'Success' OR verter.state IS NULL)
        GROUP BY checks.peer
        ORDER BY xp DESC
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_peer_max_completed_tasks('ref');
-- FETCH ALL IN "ref";
-- END;

-------------------------------------< 19 >-------------------------------------
-- Определить пира с наибольшим количеством XP.
-- Формат вывода: ник пира, количество XP.

CREATE OR REPLACE PROCEDURE get_peer_highest_amount_of_xp(ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        WITH best_xp AS (SELECT checks.peer, MAX(xp_amount) AS max
                         FROM checks
                                  JOIN xp ON checks.id = xp."check"
                         GROUP BY checks.peer, checks.task)
        SELECT best_xp.peer, SUM(best_xp.max) AS xp
        FROM best_xp
        GROUP BY best_xp.peer
        ORDER BY xp DESC
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_peer_highest_amount_of_xp('ref');
-- FETCH ALL IN "ref";
-- END;

-------------------------------------< 20 >-------------------------------------
-- Определить пира, который провел сегодня в кампусе больше всего времени.
-- Формат вывода: ник пира.

CREATE OR REPLACE PROCEDURE get_longest_time_on_campus_today(ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        WITH check_in AS (SELECT peer, SUM(time) AS sum_time_in
                          FROM time_tracking
                          WHERE date = CURRENT_DATE
                            AND state = 1
                          GROUP BY peer),
             check_out AS (SELECT peer, SUM(time) AS sum_time_out
                           FROM time_tracking
                           WHERE date = CURRENT_DATE
                             AND state = 2
                           GROUP BY peer),
             intervals AS (SELECT check_in.peer,
                                  check_out.sum_time_out - check_in.sum_time_in AS time_on_campus
                           FROM check_in
                                    JOIN check_out ON check_in.peer = check_out.peer)
        SELECT intervals.peer
        FROM intervals
        ORDER BY time_on_campus DESC
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_longest_time_on_campus_today('ref');
-- FETCH ALL IN "ref";
-- END;

-------------------------------------< 21 >-------------------------------------
-- Определить пиров, приходивших раньше заданного времени не менее N раз за всё время
-- Параметры процедуры: время, количество раз N; формат вывода: список пиров.

CREATE OR REPLACE PROCEDURE get_peer_came_before_given_time(IN given_time time,
                                                            IN N integer,
                                                            ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        WITH enter AS (SELECT peer, date, MIN(time) AS first_check_in
                       FROM time_tracking
                       WHERE state = 1
                       GROUP BY peer, date)
        SELECT peer
        FROM enter
        WHERE first_check_in < given_time
        GROUP BY peer
        HAVING COUNT(peer) > N;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_peer_came_before_given_time('14:00:00', 2, 'ref');
-- FETCH ALL IN "ref";
-- END;

-------------------------------------< 22 >-------------------------------------
-- Определить пиров, выходивших за последние N дней из кампуса больше M раз.
-- Параметры процедуры: кол-во дней N, кол-во раз M; формат вывода: список пиров.

CREATE OR REPLACE PROCEDURE get_peer_left_campus_last_N_days_more_than_M_times(IN N integer,
                                                                               IN M integer,
                                                                               ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        WITH check_out_counts
                 AS (SELECT peer, date, (COUNT(*) - 1) AS counts
                     FROM time_tracking
                     WHERE state = 2
                       AND date > CURRENT_DATE - N
                     GROUP BY peer, date)
        SELECT peer
        FROM check_out_counts
        GROUP BY peer
        HAVING SUM(counts) > M;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_peer_left_campus_last_N_days_more_than_M_times(365, 2, 'ref');
-- FETCH ALL IN "ref";
-- END;

-------------------------------------< 23 >-------------------------------------
-- Определить пира, который пришел сегодня последним (формат вывода: ник пира).

CREATE OR REPLACE PROCEDURE get_peer_who_came_last_today(ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        WITH enter AS (SELECT peer, MIN(time) AS first_check_in
                       FROM time_tracking
                       WHERE state = 1
                         AND date = CURRENT_DATE
                       GROUP BY peer)
        SELECT peer
        FROM enter
        ORDER BY first_check_in DESC
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_peer_who_came_last_today('ref');
-- FETCH ALL IN "ref";
-- END;

-------------------------------------< 24 >-------------------------------------
-- Определить пиров, которые выходили вчера из кампуса больше чем на N минут.
-- Параметры процедуры: количество минут N. Формат вывода: список пиров.

CREATE OR REPLACE PROCEDURE get_peer_left_campus_yesterday_for_more_than_N_min(IN N integer,
                                                                               ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        WITH time_out AS (SELECT peer, SUM(tmp.time_outside)::time AS time_outside
                          FROM (SELECT t1.peer                   AS peer,
                                       (t2.time - t1.time)::time AS time_outside
                                FROM (SELECT *,
                                             RANK() OVER (PARTITION BY peer ORDER BY time) AS in_out
                                      FROM time_tracking) t1
                                         JOIN (SELECT *,
                                                      RANK() OVER (PARTITION BY peer ORDER BY time) AS in_out
                                               FROM time_tracking) t2
                                              ON t1.peer = t2.peer
                                WHERE (t1.state, t2.state,
                                       t2.in_out - t1.in_out, t1.date)
                                          = (2, 1, 1, CURRENT_DATE - 1)) tmp
                          GROUP BY peer)

        SELECT time_out.peer
        FROM time_out
        WHERE time_out.time_outside > (SELECT MAKE_INTERVAL(mins => N));
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_peer_left_campus_yesterday_for_more_than_N_min(15, 'ref');
-- FETCH ALL IN "ref";
-- END;

-------------------------------------< 25 >-------------------------------------
-- Определить для каждого месяца процент ранних входов.
-- Для каждого месяца посчитать, сколько раз люди, родившиеся в этот месяц, приходили в кампус за всё время.
-- Для каждого месяца посчитать, сколько раз люди, родившиеся в этот месяц, приходили в кампус раньше 12:00 за всё время.
-- Для каждого месяца посчитать процент ранних входов в кампус относительно общего числа входов.

-- Формат вывода: месяц, процент ранних входов.

CREATE OR REPLACE PROCEDURE get_percentage_of_early_entries(ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        WITH AllEntries AS (SELECT peer, date, MIN(time) AS first_check_in
                            FROM time_tracking
                            WHERE state = 1
                            GROUP BY peer, date),
             AllBDayMonthEntries AS (SELECT m1 AS Month, COUNT(m2) AS c
                                     FROM (SELECT EXTRACT(MONTH FROM p.birthday) AS m1,
                                                  EXTRACT(MONTH FROM e.date)     AS m2
                                           FROM peers AS p
                                                    JOIN AllEntries AS e ON p.nickname = e.peer) t1
                                     WHERE m1 = m2
                                     GROUP BY m1),
             EarlyBDayMonthEntries AS (SELECT m1 AS Month, COUNT(m2) AS c
                                       FROM (SELECT EXTRACT(MONTH FROM p.birthday) AS m1,
                                                    EXTRACT(MONTH FROM e.date)     AS m2
                                             FROM peers AS p
                                                      JOIN AllEntries AS e ON p.nickname = e.peer
                                                        AND e.first_check_in < '12:00:00') t1
                                       WHERE m1 = m2
                                       GROUP BY m1)
        SELECT t1.Month, COALESCE(EarlyEntries, 0)
        FROM (SELECT EXTRACT(MONTH FROM gs) AS gs, TO_CHAR(gs, 'Month') AS Month
              FROM (SELECT GENERATE_SERIES('2022-01-01', '2022-12-01',
                                           INTERVAL '1 month') AS gs) tmp) t1
                 LEFT JOIN
             (SELECT e.Month,
                     ROUND(e.c * 100 / (a.c::numeric), 2) AS EarlyEntries
              FROM EarlyBDayMonthEntries AS e
                       JOIN AllBDayMonthEntries AS a ON a.Month = e.Month) t2
             ON t1.gs = t2.Month;
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_percentage_of_early_entries('ref');
-- FETCH ALL IN "ref";
-- CLOSE ref;
-- END;
