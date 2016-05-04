#include "IntelWeb.h"
#include "BinaryFile.h"
#include "DiskMultiMap.h"
#include <iostream> // needed for any I/O
#include <fstream>  // needed in addition to <iostream> for file I/O
#include <sstream>  // needed in addition to <iostream> for string stream I/O
#include <string>
#include <map>
#include <set>
#include <queue>
using namespace std;

IntelWeb::IntelWeb()
	:m_name1("forward.dat"),m_name2("reverse.dat")
{
}

IntelWeb::~IntelWeb()
{
	m_forwardMultiMap.close();
	m_reverseMultiMap.close();
}

bool IntelWeb::createNew(const std::string& filePrefix, unsigned int maxDataItems)
{
	string name1 = filePrefix + m_name1;
	string name2 = filePrefix + m_name2;
	
	unsigned int bucketNum = maxDataItems * 2+1;
	bool flag=true;
	if (!m_forwardMultiMap.createNew(name1, bucketNum))
		flag = false;
	if (!m_reverseMultiMap.createNew(name2, bucketNum))
		flag = false;

	if (flag == false)
	{
		m_forwardMultiMap.close();
		m_reverseMultiMap.close();
	}
	return flag;
}

bool IntelWeb::openExisting(const std::string& filePrefix)
{
	bool flag = true;
	string name1 = filePrefix + m_name1;
	string name2 = filePrefix + m_name2;
	if (!m_forwardMultiMap.openExisting(name1))
	{
		flag = false;
	}
		
	if (!m_reverseMultiMap.openExisting(name2))
	{
		flag = false;
	}

	if (flag == false)
	{
		m_forwardMultiMap.close();
		m_reverseMultiMap.close();
	}
	return flag;
}

void IntelWeb::close()
{
	m_forwardMultiMap.close();
	m_reverseMultiMap.close();
}

bool IntelWeb::ingest(const std::string& telemetryFile)
{
	
	// Open the telemetryFile for input
	ifstream inf(telemetryFile);
	// Test for failure to open
	if (!inf)
	{
		cout << "Cannot open telemetryFile file!" << endl;
		return false;
	}
	string line;
	while (getline(inf, line))
	{

		istringstream iss(line);
		string context;
		string from;
		string to;
		if (!(iss >> context >> from>> to))
		{
			cout << "Ignoring badly-formatted input line: " << line << endl;
			continue;
		}
		char dummy;
		if (iss >> dummy) // succeeds if there a non-whitespace char
			cout << "Ignoring extra data in line: " << line << endl;

		m_forwardMultiMap.insert(from, to, context);
		m_reverseMultiMap.insert(to, from, context);
	}
	return true;
	
}

int IntelWeb::count(string& value)
{
	int num=0;
	DiskMultiMap::Iterator r_it = m_reverseMultiMap.search(value);
	DiskMultiMap::Iterator f_it = m_forwardMultiMap.search(value);
	for (;r_it.isValid(); ++r_it)
		num++;
	for (;f_it.isValid(); ++f_it)
		num++;
	return num;
}

bool operator<(const InteractionTuple &a, const InteractionTuple &b)
{
	if (a.context < b.context)
		return true;
	else if (a.context>b.context)
		return false;
	else //context equails
	{
		if (a.from < b.from)
			return true;
		else if (a.from>b.from)
			return false;
		else //from equals
		{
			if (a.to < b.to)
				return true;
			else
				false;
		}
	}
}


unsigned int IntelWeb::crawl(const std::vector<std::string>& indicators,
	unsigned int minPrevalenceToBeGood,
	std::vector<std::string>& badEntitiesFound,
	std::vector<InteractionTuple>& interactions
	)
{
	std::set<InteractionTuple> set_interactions;
	std::set<InteractionTuple>::iterator set_interactions_it;
	std::set<std::string> set_badEntities;
	std::set<std::string>::iterator  set_badEntities_it;
	std::queue<string> queue_badEntities;
	map<std::string, int> Prevalence;
	map<std::string, int>::iterator map_it;
	std::vector<std::string> m_indicators= indicators;
	std::vector<std::string>::iterator vector_it = m_indicators.begin();
	for (; vector_it != m_indicators.end(); vector_it++)
	{
		set_badEntities.insert(*vector_it);
		string key= *vector_it;
//		cout << key << endl;
		int prevalence;
		f_Iter = m_forwardMultiMap.search(key);
		r_Iter = m_reverseMultiMap.search(key);

		for (; f_Iter.isValid(); ++f_Iter)
		{
			InteractionTuple t_interactionTuple;
			t_interactionTuple.context = (*f_Iter).context;
			t_interactionTuple.from = (*f_Iter).key;
			t_interactionTuple.to = (*f_Iter).value;
			set_interactions.insert(t_interactionTuple);
			string value = (*f_Iter).value;
//			cout << value <<endl;
			map_it = Prevalence.find(value);
			if (map_it == Prevalence.end())
			{
				prevalence = count(value);
				if (prevalence >= minPrevalenceToBeGood)
				{
					Prevalence[value]= prevalence;
					continue;
				}
				else
				{
					if (set_badEntities.find(value) == set_badEntities.end())
						queue_badEntities.push(value);
					set_badEntities.insert(value);

				}
			}
			else
			{
				continue;
			}
		}
		for (; r_Iter.isValid(); ++r_Iter )
		{
			set_badEntities.insert(*vector_it);
			InteractionTuple t_interactionTuple;
			t_interactionTuple.context = (*r_Iter).context;
			t_interactionTuple.from = (*r_Iter).value;
			t_interactionTuple.to = (*r_Iter).key;
			set_interactions.insert(t_interactionTuple);
			string value = (*r_Iter).value;

//			cout << value << endl;
			map_it = Prevalence.find(value);
			if (map_it == Prevalence.end())
			{
				prevalence = count(value);
				if (prevalence >= minPrevalenceToBeGood)
				{
					Prevalence[value] = prevalence;
					continue;
				}
				else
				{
					if (set_badEntities.find(value) == set_badEntities.end())
						queue_badEntities.push(value);
					set_badEntities.insert(value);
//					cout << value << endl;
//					cout << *vector_it << endl;

				}
			}
			else
			{
				continue;
			}
		}
	}
	for (; !queue_badEntities.empty(); queue_badEntities.pop())
	{
		string m_key = queue_badEntities.front();
		int prevalence;
		f_Iter = m_forwardMultiMap.search(m_key);
		r_Iter = m_reverseMultiMap.search(m_key);
		for (; f_Iter.isValid(); ++f_Iter)
		{

			InteractionTuple t_interactionTuple;
			t_interactionTuple.context = (*f_Iter).context;
			t_interactionTuple.from = (*f_Iter).key;
			t_interactionTuple.to = (*f_Iter).value;
			set_interactions.insert(t_interactionTuple);
			string value = (*f_Iter).value;
			map_it = Prevalence.find(value);
			if (map_it == Prevalence.end())
			{
				prevalence = count(value);
				if (prevalence >= minPrevalenceToBeGood)
				{
					Prevalence[value] = prevalence;
					continue;
				}
				else
				{
					if (set_badEntities.find(value) == set_badEntities.end())
						queue_badEntities.push(value);
					set_badEntities.insert(value);
				}
			}
			else
			{
				continue;
			}
		}
		for (; r_Iter.isValid(); ++r_Iter)
		{
			InteractionTuple t_interactionTuple;
			t_interactionTuple.context = (*r_Iter).context;
			t_interactionTuple.from = (*r_Iter).value;
			t_interactionTuple.to = (*r_Iter).key;
			set_interactions.insert(t_interactionTuple);
			string value = (*r_Iter).value;
			map_it = Prevalence.find(value);
			if (map_it == Prevalence.end())
			{
				prevalence = count(value);
				if (prevalence >= minPrevalenceToBeGood)
				{
					Prevalence[value] = prevalence;
//					cout << prevalence<<endl;
					continue;
				}
				else
				{
					if (set_badEntities.find(value) == set_badEntities.end())
						queue_badEntities.push(value);
					set_badEntities.insert(value);
				}
			}
			else
			{
				continue;
			}
		}
		
	}
	int num = 0;
	for (set_badEntities_it = set_badEntities.begin(); set_badEntities_it != set_badEntities.end(); set_badEntities_it++)
	{
		num++;
		badEntitiesFound.push_back(*set_badEntities_it);
	}
	for (set_interactions_it = set_interactions.begin(); set_interactions_it != set_interactions.end(); set_interactions_it++)
	{
		interactions.push_back(*set_interactions_it);
	}
	return num;
}
bool IntelWeb::purge(const std::string& entity)
{
	int num= 0;

	DiskMultiMap::Iterator f_it = m_forwardMultiMap.search(entity);
	DiskMultiMap::Iterator r_it = m_reverseMultiMap.search(entity);
	while (f_it.isValid() || r_it.isValid())
	{
		for (; f_it.isValid(); ++f_it)
		{
			string context = (*f_it).context;
			string value = (*f_it).value;
			string key = (*f_it).key;
			num += m_forwardMultiMap.erase(key, value, context);
			num += m_reverseMultiMap.erase(value, key, context);

		}
		for (; r_it.isValid(); ++r_it)
		{
			string context = (*r_it).context;
			string value = (*r_it).value;
			string key = (*r_it).key;
			num += m_forwardMultiMap.erase(value, key, context);
			num += m_reverseMultiMap.erase(key, value, context);

		}
		f_it = m_forwardMultiMap.search(entity);
		r_it = m_reverseMultiMap.search(entity);
	}
	
	return num > 0;
}