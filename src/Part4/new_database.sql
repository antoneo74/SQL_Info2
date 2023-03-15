-- Создадим новую БД.
-- Перед созданием новой БД необходимо подключиться к системной БД "postgres".
--
--CREATE DATABASE Part4
--;

-- \c Part4

CREATE TYPE check_status AS ENUM ('Start', 'Success', 'Failure');
;

-- Таблица пиров.
CREATE TABLE IF NOT EXISTS peers (
    nickname varchar PRIMARY KEY,
    birthday date
)
;
-- Таблица заданий.
CREATE TABLE IF NOT EXISTS tasks (
    title varchar PRIMARY KEY,
    parent_task varchar NULL REFERENCES tasks(title),  -- ссылаемся на другую запись из этой же таблицы
    max_xp integer
)
;
-- Таблица проверок.
CREATE TABLE IF NOT EXISTS checks (
    id bigint PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    peer varchar REFERENCES peers(nickname),
    task varchar REFERENCES tasks(title),
    "date" date
)
;
-- Таблица полученных XP (за успешно выполненные задания).
CREATE TABLE IF NOT EXISTS xp (
    id bigint PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    "check" bigint NOT NULL REFERENCES checks(id),
    xp_amount integer
)
;
-- Таблица автоматических проверок (verter).
CREATE TABLE IF NOT EXISTS verter (
    id bigint PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    "check" bigint NOT NULL REFERENCES checks(id),
    state check_status,
    "time" time
)
;
-- Таблица peer-to-peer проверок.
CREATE TABLE IF NOT EXISTS p2p (
    id bigint PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    "check" bigint NOT NULL REFERENCES checks(id),
    checking_peer varchar REFERENCES peers(nickname),
    state check_status,
    "time" time
)
;
-- Таблица движения пир-поинтов.
CREATE TABLE IF NOT EXISTS transferred_points (
    id bigint PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    checking_peer varchar REFERENCES peers(nickname),
    checked_peer varchar REFERENCES peers(nickname),
    points_amount integer
)
;
-- Таблица друзей.
CREATE TABLE IF NOT EXISTS friends (
    id bigint PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    peer1 varchar REFERENCES peers(nickname),
    peer2 varchar REFERENCES peers(nickname)
)
;
-- Таблица рекомендаций.
CREATE TABLE IF NOT EXISTS recommendations (
    id bigint PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    peer varchar REFERENCES peers(nickname),
    recommended_peer varchar REFERENCES peers(nickname)
)
;
-- Таблица учета времени.
CREATE TABLE IF NOT EXISTS time_tracking (
    id bigint PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    peer varchar REFERENCES peers(nickname),
    "date" date,
    "time" time,
    state smallint CHECK ( state BETWEEN 1 AND 2 )
)
;

--
-- Процедуры импорта/экспорта данных.
--

CREATE OR REPLACE PROCEDURE from_csv(path text, separator char = ',')
    LANGUAGE plpgsql
AS
$$
BEGIN
EXECUTE ('COPY peers FROM '''
    || path
    || '/peers.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY tasks FROM '''
    || path
    || '/tasks.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY checks (peer, task, "date") FROM '''
    || path
    || '/checks.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY xp ("check", xp_amount) FROM '''
    || path
    || '/xp.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY verter ("check", state, "time") FROM '''
    || path
    || '/verter.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY p2p ("check", checking_peer, state, "time") FROM '''
    || path
    || '/p2p.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY transferred_points (checking_peer, checked_peer, points_amount) FROM '''
    || path
    || '/transferred_points.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY friends (peer1, peer2) FROM '''
    || path
    || '/friends.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY recommendations (peer, recommended_peer) FROM '''
    || path
    || '/recommendations.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY time_tracking (peer, "date", "time", state) FROM '''
    || path
    || '/time_tracking.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
END
$$
;

CREATE OR REPLACE PROCEDURE to_csv(path text, separator char = ',')
    LANGUAGE plpgsql
AS
$$
BEGIN
EXECUTE ('COPY peers TO '''
    || path
    || '/peers.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY tasks TO '''
    || path
    || '/tasks.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY checks (peer, task, "date") TO '''
    || path
    || '/checks.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY xp ("check", xp_amount) TO '''
    || path
    || '/xp.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY verter ("check", state, "time") TO '''
    || path
    || '/verter.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY p2p ("check", checking_peer, state, "time") TO '''
    || path
    || '/p2p.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY transferred_points (checking_peer, checked_peer, points_amount) TO '''
    || path
    || '/transferred_points.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY friends (peer1, peer2) TO '''
    || path
    || '/friends.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY recommendations (peer, recommended_peer) TO '''
    || path
    || '/recommendations.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
EXECUTE ('COPY time_tracking (peer, "date", "time", state) TO '''
    || path
    || '/time_tracking.csv'' WITH (FORMAT CSV, DELIMITER '''
    || separator
    || ''')');
END
$$
;

CALL from_csv('/home/antoneo/Downloads/SQL2_Info21_v1.0-0-master/src/Part1/csv')
;

-- 1. Процедура добавления p2p-проверок.
--
CREATE OR REPLACE PROCEDURE add_p2p_check(
    checked_peer_ varchar,
    checking_peer_ varchar,
    task_ varchar,
    status_ check_status,
    time_ time)
    LANGUAGE plpgsql
AS
$$
BEGIN
    IF status_ = 'Start' THEN
        INSERT INTO checks (peer, task, date) VALUES (checked_peer_, task_, now());
        INSERT INTO p2p ("check", checking_peer, state, "time")
        VALUES ((SELECT max(id) FROM checks),
                checking_peer_, status_, time_);
    ELSE
        INSERT INTO p2p ("check", checking_peer, state, "time")
        VALUES ((SELECT c.id
                 FROM checks c
                          INNER JOIN p2p p on c.id = p."check"
                 WHERE c.peer = checked_peer_
                   AND c.task = task_
                 GROUP BY c.id, c.peer, c.task, c.date
                 HAVING count(p.id) = 1
                 ORDER BY c.date DESC
                 LIMIT 1),
                checking_peer_, status_, time_);
    END IF;
END
$$
;

-- { tests
-- Предварительно добавим одну не завершенную проверку:
INSERT INTO checks (peer, task, "date")
VALUES ('mumu', 'C3_s21_string+', '2022-05-19');
INSERT INTO p2p ("check", checking_peer, state, "time")
VALUES ((SELECT max(id) FROM checks), 'bellatri', 'Start', '12:00:00');
-- Добавим две p2p-проверки
CALL add_p2p_check('mumu', 'bellatri', 'C3_s21_string+', 'Failure', '15:35');
CALL add_p2p_check('conor', 'bellatri', 'C3_s21_string+', 'Start', '15:40');
-- } tests


-- 2. Процедура добавления проверок Вертером.
--
CREATE OR REPLACE PROCEDURE add_verter_check(
    peer_ varchar,
    task_ varchar,
    status_ check_status,
    time_ time)
    LANGUAGE plpgsql
AS
$$
BEGIN
    INSERT INTO verter ("check", state, "time")
    VALUES ((SELECT c.id
             FROM checks c
                      INNER JOIN p2p p on c.id = p."check"
             WHERE c.peer = peer_
               AND c.task = task_
               AND p.state = 'Success'
             ORDER BY c.date DESC
             LIMIT 1),
            status_, time_);
END
$$
;

-- { tests
-- Предварительно завершим предыдущую p2p-проверку успехом.
CALL add_p2p_check('conor', 'bellatri', 'C3_s21_string+', 'Success', '16:00');
-- Добавим успешную verter-проверку
CALL add_verter_check('conor', 'C3_s21_string+', 'Start', '17:00');
CALL add_verter_check('conor', 'C3_s21_string+', 'Success', '17:10');
-- Попытаемся добавить verter-проверку для НЕ успешной p2p-проверки.
-- CALL add_verter_check('mumu', 'C3_s21_string+', 'Start', '18:00');
-- } tests


-- 3. Триггер для передачи пир-поинтов.
--
CREATE OR REPLACE FUNCTION on_p2p_start() RETURNS TRIGGER
    LANGUAGE plpgsql
AS
$on_p2p_start$
BEGIN
    INSERT INTO transferred_points (checking_peer, checked_peer, points_amount)
    VALUES (NEW.checking_peer,
            (SELECT peer FROM checks WHERE id = NEW.check),
            1);
    RETURN NULL;
END
$on_p2p_start$
;
CREATE OR REPLACE TRIGGER trg_p2p_start_handler
    AFTER INSERT
    ON p2p
    FOR EACH ROW
    WHEN ( NEW.state = 'Start' )
EXECUTE FUNCTION on_p2p_start()
;

-- { tests
-- Добавим новую p2p-проверку.
CALL add_p2p_check('conor', 'papuas', 'C4_s21_math', 'Start', '15:05');
-- Убедимся, что триггер сработал.
SELECT *
FROM transferred_points
WHERE checked_peer = 'conor'
  AND checking_peer = 'papuas';
-- } tests


-- 4. Триггер для валидации начисления XP.
--
CREATE OR REPLACE FUNCTION validate_xp() RETURNS TRIGGER
    LANGUAGE plpgsql
AS
$validate_xp$
BEGIN
    IF NEW.xp_amount > (SELECT max_xp
                        FROM tasks t
                                 INNER JOIN checks c on t.title = c.task
                        WHERE c.id = NEW.check) THEN
        RETURN NULL;
    END IF;
    IF 0 = (SELECT count(*)
            FROM checks c
                     INNER JOIN p2p p on c.id = p."check"
                     LEFT JOIN verter v on c.id = v."check"
            WHERE c.id = NEW.check
              AND p.state = 'Success'
              AND (v.state = 'Success' OR v.state IS NULL)) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END
$validate_xp$
;
CREATE OR REPLACE TRIGGER trg_validate_xp
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE FUNCTION validate_xp()
;

-- { tests
-- Убедимся, что триггер сработал корректно для всех случаев:

-- a) правильное кол-во XP за успешную проверку
INSERT INTO xp ("check", xp_amount)
VALUES ((SELECT min(p."check")
         FROM p2p p
                  LEFT JOIN verter v on p."check" = v."check"
         WHERE p.state = 'Success'
           AND (v.state = 'Success' OR v.state IS NULL)),
        250);

-- b) превышено макс. кол-во XP за успешную проверку
INSERT INTO xp ("check", xp_amount)
VALUES ((SELECT min(p."check")
         FROM p2p p
                  LEFT JOIN verter v on p."check" = v."check"
         WHERE p.state = 'Success'
           AND (v.state = 'Success' OR v.state IS NULL)),
        1000);

-- c) правильное кол-во XP за НЕ успешную p2p-проверку
INSERT INTO xp ("check", xp_amount)
VALUES ((SELECT min("check")
         FROM p2p
         WHERE state = 'Failure'),
        250);

-- d) правильное кол-во XP за НЕ успешную verter-проверку
INSERT INTO xp ("check", xp_amount)
VALUES ((SELECT min(p."check")
         FROM p2p p
                  LEFT JOIN verter v on p."check" = v."check"
         WHERE p.state = 'Success'
           AND v.state = 'Failure'),
        250);
-- } tests
