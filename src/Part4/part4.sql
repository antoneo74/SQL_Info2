-- запустить скрипт из файла new_database.sql для создания новой базы данных

--1) Создать хранимую процедуру, которая, не уничтожая базу данных, уничтожает все те таблицы текущей базы данных,
-- имена которых начинаются с фразы 'TableName'.

create or replace procedure drop_table_like(name varchar) as
$$
declare
    i text;
begin
    for i in (select tablename
              from pg_tables
              where tablename like name || '%'
                AND schemaname = 'public')
        loop
            execute 'DROP TABLE ' || i || ' cascade;';

        end loop;

end;
$$ language plpgsql;

--call drop_table_like('v');

--2) Создать хранимую процедуру с выходным параметром, которая выводит список имен и параметров
-- всех скалярных SQL функций пользователя в текущей базе данных. Имена функций без параметров не выводить.
-- Имена и список параметров должны выводиться в одну строку. Выходной параметр возвращает количество найденных функций.

create or replace procedure find_scalar_functions(ref refcursor, out count_scalar int) as
$$
begin
    create view result as
    (
    with tmp as
             (select routine_name, ordinal_position, parameter_mode, parameter_name, t2.data_type
              from (select *
                    from information_schema.routines
                    where specific_schema = ('public')
                      and routine_type = 'FUNCTION') t1
                       join
                   (select *
                    from information_schema.parameters
                    where specific_schema = ('public')
                      and parameter_name is not null) t2
                   on t1.specific_name = t2.specific_name
              order by 1, 2)

    select routine_name || '(' || array_to_string(array_agg(data_type), ',') || ')' as scalar_function
    from tmp
    group by routine_name);

    count_scalar = (select count(*) count_scalar
                    from result);

    open ref for
        select * from result;

    drop view result;

end;
$$ language plpgsql;


--begin;
--call find_scalar_functions('ref', null);
--fetch all in "ref";
--close ref;
--commit;

--3) Создать хранимую процедуру с выходным параметром, которая уничтожает все SQL DML триггеры в текущей базе данных.
-- Выходной параметр возвращает количество уничтоженных триггеров.

create or replace procedure drop_triggers(out count_dropped int) as
$$
declare
    i   text;
    tab text;
begin
    select count(*)
    into count_dropped
    from information_schema.triggers
    where (trigger_schema, event_object_schema) = ('public', 'public');

    for i, tab in (select trigger_name, event_object_table
                   from information_schema.triggers
                   where (trigger_schema, event_object_schema) = ('public', 'public'))
        loop
            execute 'DROP TRIGGER ' || i || ' ON ' || tab || ';';
        end loop;

end;
$$ language plpgsql;

--call drop_triggers(null);

--4) Создать хранимую процедуру с входным параметром, которая выводит имена и описания типа объектов
-- (только хранимых процедур и скалярных функций), в тексте которых на языке SQL встречается строка,
-- задаваемая параметром процедуры.

create or replace procedure find_routines_have_code_like(in code varchar, ref refcursor) as
$$
begin
    open ref for
        select routine_name, routine_type
        from information_schema.routines
        where routine_type in ('PROCEDURE', 'FUNCTION')
          and routine_schema = 'public'
          and routine_definition like '%' || code || '%';
end;
$$ LANGUAGE plpgsql;


--begin;
--call find_routines_have_code_like('INSERT INTO', 'ref');
--fetch all in "ref";
--close  ref;
--commit;
