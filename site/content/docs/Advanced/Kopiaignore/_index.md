---
title: "Ignoring Files and Folders in Snapshots"
linkTitle: "Ignoring Files and Folders in Snapshots"
weight: 40
---

## Ignoring Files and Folders in Snapshots

Users may want to exclude folders and files not to be saved within the repository when creating snapshots. The benefits of omitting unnecessary files and folders are smaller and faster snapshots while saving only essential data. 

Kopia uses `pattern-based` ignore rules to omit folders and files from snapshots. While scanning directories and their content, Kopia looks explicitly for files that contain such rules. 
If such a file is placed within a directory, `Kopia` omits files and folders `matching` the rules.

>NOTE The default file is called `.kopiaignore`. However, ignore rules can be specified within the global or snapshot-specific `policy` - either directly or by providing a path to file containing such rules.   

In the following, we explain different rules and provide examples to create `.kopiaignore` files.

### Kopiaignore Files

Let us start with an example directory that contains the following files and folders:

```shell
thesis/
  --title.png
  --manuscript.tex
  --figures/
	--architecture.png
	--server.png
  --chapters/
	--introduction.tex
	--abstract.tex
	--conclusion.tex
	--logs/
	    --chapter.log
  --logs/
	--gen.log
	--fail.log
	--log.db
	--tmp.db
	--tmp.dba
  --tmp.db
  --atmp.db
  --abtmp.db
  --logs.dat
```

The above directory consists of a bunch of `tex` files, figures, and temporary files. Generally, a `.kopiaignore` file is a simple text file where each line represents a single rule. To only save the essential files, we create the following `.kopiaignore` file:

```shell
# Ignoring all files that end with ".dat"
*.dat

# Ignoring all files and folders within the "thesis/logs" directory
/logs/*

# Ignoring "tmp.db" files within the whole directory
tmp.db
```

The example above contains three simple rules to exclude files and folders from a `snapshot` and some comments. 
Each line that begins with a `#` is a `comment` and can be used to describe the rule. 

* The first rule, `*.dat` contains a `wildcard` and ignores all files with a filename that ends with `.dat`. 
* The second rule, `/logs/*` ignores all files within the `logs` directory. Only the `logs` directory at the `root` will be ignored as the rule begins with a `/`.
* The third rule, `tmp.db` ignores the corresponding files within the whole directory. In our example both `tmp.db` files will be ignored. 

The example shows that excluding files using `.kopiaignore` from a snapshot is easy. However, there is also the risk of accidentally excluding files when creating rule - leading to incomplete snapshots or data loss. 
 
### Supported Patterns

`Kopia` supports a lot of different operators allowing users to precisely exclude unnecessary files or folders. The following table shows special operators used to generate rules.

| **Special Operator**	| **Explanation**												|
|-----------------------|---------------------------------------------------------------|
| `#`					| `Comment` that is ignored by `Kopia`							|
| `!`					| `Negates` a following rule									|
| `*`					| `Wildcard` that matches any character zero or multiple times	|
| `**`					| Double `Wildcard` that matches zero or multiple directories	|
| `?`					| Matches any character exactly `one` time						|
| `[0-9]`				| Matches any single number between `0` and `9`					|
| `[a-z]`				| Matches any single character between `a` and `z`				|
| `[A-Z]`				| Matches any single character between `A` and `Z`				|
| `[abc]`				| Matches one of `a`, `b`, or `c`								|
| `/`					| Matches a following rule only at the `root` directory		|

### Examples of Kopiaignore Rules 

The following table provides some example rules related to our [example](#kopiaignore-files). Files and folders that `match` the given rules are excluded from the snapshot.

| **Rule**			| **Explanation**																												| **Matches**													|**Ignores**							|
|-----------------------|-------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------|---------------------------------------|
| `logs`				| Matches files and folders that are named `logs`																				| thesis/logs/ </br> thesis/chapters/logs/						| 2 directories, 6 files				|
| `/logs`				| Matches files and folders that are named `logs` only within the parent directory												| thesis/logs/													| 1 directory, 5 files					|   
| `*.db`				| Matches files with extension `.db`																							| (...) </br> thesis/tmp.db </br> thesis/logs/log.db			| 0 directories, 5 files				|
| `*.db*`				| Matches files with extension `.db` followed by any other number or character													| (...) </br> thesis/tmp.db </br> thesis/logs/tmp.dba			| 0 directories, 6 files				|
| `**/logs/**`			| Matches all occurrences of `logs` within the `thesis` and sub-directories  													| (...) </br> thesis/logs/ </br> thesis/chapters/logs/			| 2 directories, 6 files				|
| `chapters/**/*.log`	| Matches all files with extension `.log` in all sub-directories within `chapters` 												| thesis/chapters/logs/chapter.log								| 0 directories, 1 file					|      
| `*.*`					| Matches all files in `thesis`																									| (...) </br> thesis/ </br> thesis/tmp.db						| 5 directories, 17 files (all)			|
| `!*.*`				| Matches no files in `thesis`																									| -																| 0 directories, 0 files				|
| `[a-z]?tmp.db`		| Matches files beginning with characters between `a` and `z`, followed by a single character, ending with `tmp.db`				| thesis/abtmp.db												| 0 directories, 1 file					|
| `?tmp.db`				| Matches files with exactly one character ending with `tmp.db`																	| thesis/atmp.db												| 0 directories, 1 file					|
| `[a-z]*tmp.db`		| Matches files beginning with characters between `a` and `z`, followed by zero or multiple characters, ending with `tmp.db`	| thesis/abtmp.db </br> thesis/atmp.db </br> thesis/logs/tmp.db	| 0 directories, 3 files				|

>NOTE Make sure that you have tested your `.kopiaignore` file and the resulting snapshot for correctness. If a file or folder is missing, you will need to adjust the rules to your needs.
