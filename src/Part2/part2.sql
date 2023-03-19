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
-- INSERT INTO checks (peer, task, "date")
-- VALUES ('mumu', 'C3_s21_string+', '2022-05-19');
-- INSERT INTO p2p ("check", checking_peer, state, "time")
-- VALUES ((SELECT max(id) FROM checks), 'bellatri', 'Start', '12:00:00');
-- -- Добавим две p2p-проверки
-- CALL add_p2p_check('mumu', 'bellatri', 'C3_s21_string+', 'Failure', '15:35');
-- CALL add_p2p_check('conor', 'bellatri', 'C3_s21_string+', 'Start', '15:40');
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
-- CALL add_p2p_check('conor', 'bellatri', 'C3_s21_string+', 'Success', '16:00');
-- -- Добавим успешную verter-проверку
-- CALL add_verter_check('conor', 'C3_s21_string+', 'Start', '17:00');
-- CALL add_verter_check('conor', 'C3_s21_string+', 'Success', '17:10');
-- Попытаемся добавить verter-проверку для НЕ успешной p2p-проверки.
-- CALL add_verter_check('mumu', 'C3_s21_string+', 'Start', '18:00');
-- } tests


-- 3. Триггер для передачи пир-поинтов.
--
CREATE OR REPLACE FUNCTION func_on_p2p_start() RETURNS TRIGGER
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
EXECUTE FUNCTION func_on_p2p_start()
;

-- { tests
-- Добавим новую p2p-проверку.
-- CALL add_p2p_check('conor', 'papuas', 'C4_s21_math', 'Start', '15:05');
-- -- Убедимся, что триггер сработал.
-- SELECT *
-- FROM transferred_points
-- WHERE checked_peer = 'conor'
--   AND checking_peer = 'papuas';
-- } tests


-- 4. Триггер для валидации начисления XP.
--
CREATE OR REPLACE FUNCTION func_validate_xp() RETURNS TRIGGER
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
EXECUTE FUNCTION func_validate_xp()
;

-- { tests
-- Убедимся, что триггер сработал корректно для всех случаев:

-- a) правильное кол-во XP за успешную проверку
-- INSERT INTO xp ("check", xp_amount)
-- VALUES ((SELECT min(p."check")
--          FROM p2p p
--                   LEFT JOIN verter v on p."check" = v."check"
--          WHERE p.state = 'Success'
--            AND (v.state = 'Success' OR v.state IS NULL)),
--         250);
--
-- -- b) превышено макс. кол-во XP за успешную проверку
-- INSERT INTO xp ("check", xp_amount)
-- VALUES ((SELECT min(p."check")
--          FROM p2p p
--                   LEFT JOIN verter v on p."check" = v."check"
--          WHERE p.state = 'Success'
--            AND (v.state = 'Success' OR v.state IS NULL)),
--         1000);
--
-- -- c) правильное кол-во XP за НЕ успешную p2p-проверку
-- INSERT INTO xp ("check", xp_amount)
-- VALUES ((SELECT min("check")
--          FROM p2p
--          WHERE state = 'Failure'),
--         250);
--
-- -- d) правильное кол-во XP за НЕ успешную verter-проверку
-- INSERT INTO xp ("check", xp_amount)
-- VALUES ((SELECT min(p."check")
--          FROM p2p p
--                   LEFT JOIN verter v on p."check" = v."check"
--          WHERE p.state = 'Success'
--            AND v.state = 'Failure'),
--         250);
-- } tests
