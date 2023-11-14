The crawling process includes 3 steps:
- Firstly, quests from QuestN are collected in to `data/questn/quests.json`.
- Then each quest's id will be used to get quest's user information including twitter username. Quest's users information is stored at `data/questn/quest_users/<quest_id>.json`
- All the users that are collected is then combined into `data/questn/all_users.json`
- Finnally, tweets of the specific username is scraped by `tweet-ns` and saved into `data/twitter/<username>.json`

The Structure of each json file will follow the structure given by the api or the module