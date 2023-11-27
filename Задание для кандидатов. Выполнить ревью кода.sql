/*
	К строке кода, в которой обнаружена ошибка, добавляется комментарий.
	Комментарии нумеруются.
	В комментарии указывается, какие правила кодстайла были нарушены
*/
create procedure syn.usp_ImportFileCustomerSeasonal --1 При наличии только одного параметра, этот параметр пишется на строке выполнения
	@ID_Record int 
AS -- 2. Ключевые слова, названия системных функций и все операторы пишутся со строчной буквы
set nocount on --3. Алиас задается без переносов
begin
	declare @RowCount int = (select count(*) from syn.SA_CustomerSeasonal)
	declare @ErrorMessage varchar(max) /* 4. Для объявления переменных declare используется один раз. 
	Дальнейшее переменные перечисляются через запятую с новой строки, если явно не требуется писать declare*/

-- Проверка на корректность загрузки --5 комментария к строке/блоку отступ такой же, как у строки/блока, к которому написан комментарий
	if not exists (
	select 1 --6. В условных операторах весь блок смещается на 1 отступ
	from syn.ImportFile as f -- 7. Наименование алиасов определяется согласно стандарту
	where f.ID = @ID_Record
		and f.FlagLoaded = cast(1 as bit)
	)
		begin
			set @ErrorMessage = 'Ошибка при загрузке файла, проверьте корректность данных'

			raiserror(@ErrorMessage, 3, 1)
			return --8 Пустая строка перед return
		end

	--Чтение из слоя временных данных -- 9. Между -- и комментарием есть один пробел
	select
		c.ID as ID_dbo_Customer
		,cst.ID as ID_CustomerSystemType
		,s.ID as ID_Season
		,cast(cs.DateBegin as date) as DateBegin
		,cast(cs.DateEnd as date) as DateEnd
		,c_dist.ID as ID_dbo_CustomerDistributor
		,cast(isnull(cs.FlagActive, 0) as bit) as FlagActive
	into #CustomerSeasonal
	from syn.SA_CustomerSeasonal cs -- 10. Алиас обязателен для объекта и задается с помощью ключевого слова as
		join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
			and c.ID_mapping_DataSource = 1
		join dbo.Season as s on s.Name = cs.Season
		join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor
			and cd.ID_mapping_DataSource = 1
		join syn.CustomerSystemType as cst on cs.CustomerSystemType = cst.Name -- 11. При соединение двух таблиц, сперва после on указываем поле присоединяемой таблицы
	where try_cast(cs.DateBegin as date) is not null
		and try_cast(cs.DateEnd as date) is not null
		and try_cast(isnull(cs.FlagActive, 0) as bit) is not null

	-- Определяем некорректные записи
	-- Добавляем причину, по которой запись считается некорректной -- 12. Для комментариев в несколько строк используется конструкция /* */
	select
		cs.*
		,case
			when c.ID is null then 'UID клиента отсутствует в справочнике "Клиент"' --13. Результат на 1 отступ от when
			when c_dist.ID is null then 'UID дистрибьютора отсутствует в справочнике "Клиент"'
			when s.ID is null then 'Сезон отсутствует в справочнике "Сезон"'
			when cst.ID is null then 'Тип клиента отсутствует в справочнике "Тип клиента"'
			when try_cast(cs.DateBegin as date) is null then 'Невозможно определить Дату начала'
			when try_cast(cs.DateEnd as date) is null then 'Невозможно определить Дату окончания'
			when try_cast(isnull(cs.FlagActive, 0) as bit) is null then 'Невозможно определить Активность'
		end as Reason
	into #BadInsertedRows
	from syn.SA_CustomerSeasonal as cs
	left join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer -- 14. Все виды join пишутся с 1 отступом
		and c.ID_mapping_DataSource = 1
	left join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor and c_dist.ID_mapping_DataSource = 1 -- 20. Если есть and , то выравнивать его на 1 табуляцию от join
	left join dbo.Season as s on s.Name = cs.Season
	left join syn.CustomerSystemType as cst on cst.Name = cs.CustomerSystemType
	where cc.ID is null
		or cd.ID is null
		or s.ID is null
		or cst.ID is null
		or try_cast(cs.DateBegin as date) is null
		or try_cast(cs.DateEnd as date) is null
		or try_cast(isnull(cs.FlagActive, 0) as bit) is null

	-- Обработка данных из файла
	merge into syn.CustomerSeasonal as cs
	using (
		select
			cs_temp.ID_dbo_Customer
			,cs_temp.ID_CustomerSystemType
			,cs_temp.ID_Season
			,cs_temp.DateBegin
			,cs_temp.DateEnd
			,cs_temp.ID_dbo_CustomerDistributor
			,cs_temp.FlagActive
		from #CustomerSeasonal as cs_temp
	) as s on s.ID_dbo_Customer = cs.ID_dbo_Customer
		and s.ID_Season = cs.ID_Season
		and s.DateBegin = cs.DateBegin
	when matched 
		and t.ID_CustomerSystemType <> s.ID_CustomerSystemType then
		update -- 15. При написании update/delete запроса, необходимо использовать конструкцию с from
		set -- 16. Перечисление всех полей с новой строки и одним отступом
			ID_CustomerSystemType = s.ID_CustomerSystemType
			,DateEnd = s.DateEnd
			,ID_dbo_CustomerDistributor = s.ID_dbo_CustomerDistributor
			,FlagActive = s.FlagActive
	when not matched then
		insert (ID_dbo_Customer, ID_CustomerSystemType, ID_Season, DateBegin, DateEnd, ID_dbo_CustomerDistributor, FlagActive)
		values (s.ID_dbo_Customer, s.ID_CustomerSystemType, s.ID_Season, s.DateBegin, s.DateEnd, s.ID_dbo_CustomerDistributor, s.FlagActive) -- 17. В примере аргументы идут со след. строки с отступом
	;

	-- Информационное сообщение
	begin
		select @ErrorMessage = concat('Обработано строк: ', @RowCount)
		-- 18. Пустыми строками отделяются разные логические блоки кода
		raiserror(@ErrorMessage, 1, 1)

		-- Формирование таблицы для отчетности
		select top 100 -- 19. Аргумент функции должен быть в скобках
			Season as 'Сезон'
			,UID_DS_Customer as 'UID Клиента'
			,Customer as 'Клиент'
			,CustomerSystemType as 'Тип клиента'
			,UID_DS_CustomerDistributor as 'UID Дистрибьютора'
			,CustomerDistributor as 'Дистрибьютор'
			,isnull(format(try_cast(DateBegin as date), 'dd.MM.yyyy', 'ru-RU'), DateBegin) as 'Дата начала'
			,isnull(format(try_cast(DateEnd as date), 'dd.MM.yyyy', 'ru-RU'), DateEnd) as 'Дата окончания'
			,FlagActive as 'Активность'
			,Reason as 'Причина'
		from #BadInsertedRows

		return
	end

end
