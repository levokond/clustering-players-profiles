<!-- Generated by SpecStory -->

# Understanding column renaming in dataset (2025-06-16 16:03:22)

_**User**_

why because of this line both of the last two columns are renamed to category

---

_**Assistant**_



---

Проблема возникает из-за того, как pandas обрабатывает индексы столбцов в этом DataFrame. 

Посмотрев на первоначальный вывод DataFrame, можно увидеть, что у вас многоуровневая структура заголовков. Столбцы имеют названия типа `"Unnamed: 0_level_0"`, `"Playing Time"`, `"Performance"` и т.д.

Проблема в том, что когда вы обращаетесь к `team_stats.columns[32]` и `team_stats.columns[33]`, вы получаете индексы столбцов, которые могут указывать на одинаковые названия или pandas может интерпретировать их как дублирующиеся имена.

Чтобы исправить это, рекомендую:

---



---

Давайте сначала посмотрим на текущее содержимое ячейки:

---

Read file: team_stats_cleaning.ipynb

---

Теперь обновлю ячейку 1 для диагностики проблемы:

---



---

Проблема заключается в том, что **оба последних столбца изначально имели одинаковые названия**. 

Скорее всего, при загрузке CSV файла pandas обнаружил дублирующиеся названия столбцов и автоматически переименовал их, добавив суффиксы (например, "Category" и "Category.1" или что-то подобное).

Когда вы выполняете:
```python
team_stats.rename(columns={team_stats.columns[32]: 'League', team_stats.columns[33]: 'Category'}, inplace=True)
```

Pandas применяет переименование одновременно для обоих столбцов, но если исходные названия были одинаковыми или очень похожими, результат может быть неожиданным.

**Решение:** Переименовывайте столбцы по отдельности или используйте более точную индексацию:

---



---

Основная причина проблемы в том, что:

1. **Дублирующиеся названия столбцов** - при загрузке CSV pandas может создать одинаковые или очень похожие названия для последних столбцов
2. **Одновременное переименование** - когда вы пытаетесь переименовать несколько столбцов с похожими названиями одновременно, pandas может запутаться в том, какой именно столбец переименовывать
3. **Индексация** - `team_stats.columns[32]` и `team_stats.columns[33]` могут указывать на столбцы с одинаковыми названиями

**Рекомендуемое решение:** использовать прямое присваивание через список или переименовывать столбцы по одному, используя более специфичные методы идентификации.

---

