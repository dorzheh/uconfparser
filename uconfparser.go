/*
Copyright (c) 2013, Alex Yu <alex@alexyu.se>
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the <organization> nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.




Copyright (c) 2014, Dmitry Orzhehovsky <dorzheh@gmail.com>
a major refactoring.The following stuff is added:
- multiple similar keys
- retaining comments while parsing,binding the comments to appropriate section
- choosing which delimeter will be used when splitting key/value 
- ability for providing your own delimeter between key and value when saving to file
- choosing section header according to a regular expression
Refer to the README.md file for the package description

*/


package uconfparser

import (
	"bufio"
	"container/list"
	"errors"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
)

// Configuration represents a configuration file with its sections and options.
type Configuration struct {
	filePath        string                // configuration file
	sections        map[string]*list.List // fully qualified section name as key
	orderedSections []string              // track the order of section names as they are parsed
	mutex           sync.RWMutex
}

// A Section in a configuration
type Section struct {
	fqn            string
	options        map[string][]string
	printSection   bool     // shall we print the section line
	delimeter      string   // delimeter between key and value
	orderedOptions []string // track the order of the options as they are parsed
	mutex          sync.RWMutex
}

// NewConfiguration returns a new Configuration instance with an empty file path.
func NewConfiguration() *Configuration {
	return newConfiguration("")
}

// Parse parses a specified configuration file and returns a Configuration instance.
// Comments arn't being stripped
func Parse(filePath string, sectionExpr string, delimeter string, submatch bool) (*Configuration, error) {
	filePath = path.Clean(filePath)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	config := newConfiguration(filePath)
	activeSection := config.addSection("global")
	secexp := regexp.MustCompile(sectionExpr)
	scanner := bufio.NewScanner(bufio.NewReader(file))
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 {
			switch {
			case submatch:
				sub := secexp.FindStringSubmatch(line)
				if len(sub) > 1 {
					activeSection = config.addSection(sub[1])
					activeSection.printSection = false
				}
			//use the whole line as a section
			case secexp.MatchString(line):
				activeSection = config.addSection(line)
				activeSection.printSection = false
			case isSection(line):
				fqn := strings.Trim(line, " []")
				activeSection = config.addSection(fqn)
				activeSection.printSection = true
			}
			activeSection.delimeter = delimeter
			addOption(activeSection, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return config, nil
}

// Save the Configuration to file. Creates a backup (.bak) if file already exists.
func Save(c *Configuration, filePath string) (err error) {
	c.mutex.Lock()
	err = os.Rename(filePath, filePath+".bak")
	if err != nil {
		if !os.IsNotExist(err) { // fine if the file does not exists
			return err
		}
	}
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer func() {
		err = f.Close()
	}()
	w := bufio.NewWriter(f)
	defer func() {
		err = w.Flush()
	}()
	c.mutex.Unlock()

	s, err := c.AllSections()
	if err != nil {
		return err
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, v := range s {
		w.WriteString(v.String())
		w.WriteString("\n")
	}

	return err
}

// NewSection creates and adds a new Section with the specified name.
func (c *Configuration) NewSection(fqn string) *Section {
	return c.addSection(fqn)
}

// Filepath returns the configuration file path.
func (c *Configuration) FilePath() string {
	return c.filePath
}

// SetFilePath sets the Configuration file path.
func (c *Configuration) SetFilePath(filePath string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.filePath = filePath
}

// Values returns the values for the specified section and option.
func (c *Configuration) Values(section, option string) (values []string, err error) {
	s, err := c.Section(section)
	if err != nil {
		return
	}
	values = s.ValuesOf(option)
	return
}

// Delete deletes the specified sections matched by a regex name and returns the deleted sections.
func (c *Configuration) Delete(regex string) (sections []*Section, err error) {
	sections, err = c.Find(regex)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if err == nil {
		for _, s := range sections {
			delete(c.sections, s.fqn)
		}
		// remove also from ordered list
		var matched bool
		for i, name := range c.orderedSections {
			if matched, err = regexp.MatchString(regex, name); matched {
				c.orderedSections = append(c.orderedSections[:i], c.orderedSections[i+1:]...)
			} else {
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return sections, err
}

// Section returns the first section matching the fully qualified section name.
func (c *Configuration) Section(fqn string) (*Section, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if l, ok := c.sections[fqn]; ok {
		for e := l.Front(); e != nil; e = e.Next() {
			s := e.Value.(*Section)
			return s, nil
		}
	}
	return nil, errors.New("Unable to find " + fqn)
}

// AllSections returns a slice of all sections available.
func (c *Configuration) AllSections() ([]*Section, error) {
	return c.Sections("")
}

// Sections returns a slice of Sections matching the fully qualified section name.
func (c *Configuration) Sections(fqn string) ([]*Section, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	var sections []*Section

	f := func(lst *list.List) {
		for e := lst.Front(); e != nil; e = e.Next() {
			s := e.Value.(*Section)
			sections = append(sections, s)
		}
	}
	if fqn == "" {
		// Get all sections.
		for _, fqn := range c.orderedSections {
			if lst, ok := c.sections[fqn]; ok {
				f(lst)
			}
		}
	} else {
		if lst, ok := c.sections[fqn]; ok {
			f(lst)
		} else {
			return nil, errors.New("Unable to find " + fqn)
		}
	}
	return sections, nil
}

// Find returns a slice of Sections matching the regexp against the section name.
func (c *Configuration) Find(regex string) ([]*Section, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	var sections []*Section
	for key, lst := range c.sections {
		if matched, err := regexp.MatchString(regex, key); matched {
			for e := lst.Front(); e != nil; e = e.Next() {
				s := e.Value.(*Section)
				sections = append(sections, s)
			}
		} else {
			if err != nil {
				return nil, err
			}
		}
	}
	return sections, nil
}

// PrintSection prints a text representation of all sections matching the fully qualified section name.
func (c *Configuration) PrintSection(fqn string) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	sections, err := c.Sections(fqn)
	if err == nil {
		for _, section := range sections {
			fmt.Print(section)
		}
	} else {
		fmt.Printf("Unable to find section %v\n", err)
	}
}

// String returns the text representation of a parsed configuration file.
func (c *Configuration) String() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	var parts []string
	for _, fqn := range c.orderedSections {
		sections, _ := c.Sections(fqn)
		for _, section := range sections {
			parts = append(parts, section.String())
		}
	}
	return strings.Join(parts, "")
}

// Exists returns true if the option exists
func (s *Section) Exists(option string) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	_, ok := s.options[option]
	return ok
}

// ValueOf returns the value of specified option.
func (s *Section) ValuesOf(option string) []string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.options[option]
}

// ReplaceValueFor sets the value for the specified option and returns the old value.
func (s *Section) ReplaceValueFor(option string, oldValue, newValue string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if values, ok := s.options[option]; ok {
		if oldValue == "" && len(values) > 0 {
			values[0] = newValue
			return true
		}
		for i, value := range values {
			if value == oldValue {
				values[i] = newValue
				return true
			}
		}
	}
	return false
}

// AddValueFor adds the value for the specified option and returns the slice of values.
func (s *Section) AddValueFor(option string, newValue string) []string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.options[option] = append(s.options[option], newValue)
	return s.options[option]
}

// Add adds a new option to the section. Adding and existing option will overwrite the old one.
// The old value is returned
func (s *Section) Add(option string, newValue string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	var values []string
	var ok bool
	if values, ok = s.options[option]; !ok {
		s.orderedOptions = append(s.orderedOptions, option)
	}
	for _, value := range values {
		if value == newValue {
			return false
		}
	}
	s.options[option] = append(s.options[option], newValue)
	return true
}

// DeleteOption removes the specified option from the section and returns the deleted option's values.
func (s *Section) DeleteOption(option string) (values []string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	values = s.options[option]
	delete(s.options, option)
	for i, opt := range s.orderedOptions {
		if opt == option {
			s.orderedOptions = append(s.orderedOptions[:i], s.orderedOptions[i+1:]...)
		}
	}
	return
}

// Delete removes the specified option from the section and returns the deleted option's values.
func (s *Section) DeleteOptionAndValue(option, value string) (values []string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	values = s.options[option]
	
	for i, val := range values {
		if val == value {
			values = append(values[:i], values[i+1:]...)
		}
	}
	if len(values) > 0 {
		s.options[option] = values
	} else {
		delete(s.options, option)
		for i, opt := range s.orderedOptions {
			if opt == option {
				s.orderedOptions = append(s.orderedOptions[:i], s.orderedOptions[i+1:]...)
			}
		}
	}
	return
}

// Options returns a map of options for the section.
func (s *Section) Options() map[string][]string {
	return s.options
}

// OptionNames returns a slice of option names in the same order as they were parsed.
func (s *Section) OptionNames() []string {
	return s.orderedOptions
}

// String returns the text representation of a section with its options.
func (s *Section) String() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	var parts []string
	// dmitryo: don't print section name if we don't need it
	var s_name string
	if s.fqn == "global" || s.printSection == false {
		s_name = ""
	} else {
		s_name = "[" + s.fqn + "]\n"
	}
	parts = append(parts, s_name)
	for _, opt := range s.orderedOptions {
		values := s.options[opt]
		if len(values) != 0 {
			for _, value := range values {
				if value != "" {
					parts = append(parts, opt, s.delimeter, value, "\n")
				} else {
					parts = append(parts, opt, "\n")
				}
			}
		} else {
			parts = append(parts, opt, "\n")
		}
	}
	return strings.Join(parts, "")
}

//
// Private
//

// newConfiguration creates a new Configuration instance.
func newConfiguration(filePath string) *Configuration {
	return &Configuration{
		filePath: filePath,
		sections: make(map[string]*list.List),
	}
}

func isSection(section string) bool {
	return strings.HasPrefix(section, "[")
}

func addOption(s *Section, option string) {
	addToOdered := true
	opt, value := parseOption(option)
	if _, ok := s.options[opt]; ok {
		addToOdered = false
	}
	if value != "" {
		s.options[opt] = append(s.options[opt], value)
	} else {
		// only insert keys. ex list of hosts
		s.options[opt] = append(s.options[opt], "")
	}
	if addToOdered {
		s.orderedOptions = append(s.orderedOptions, opt)
	}
}

func parseOption(option string) (opt, value string) {
	split := func(i int, delim string) (opt, value string) {
		// strings.Split cannot handle wsrep_provider_options settings
		opt = strings.Trim(option[:i], " ")
		value = strings.Trim(option[i+1:], " ")
		return
	}
	if i := strings.Index(option, "="); i != -1 {
		opt, value = split(i, "=")
	} else if i := strings.Index(option, ":"); i != -1 {
		opt, value = split(i, ":")
	} else {
		opt = option
	}
	return
}

func (c *Configuration) addSection(fqn string) *Section {
	section := &Section{fqn: fqn, options: make(map[string][]string)}
	var lst *list.List
	if lst = c.sections[fqn]; lst == nil {
		lst = list.New()
		c.sections[fqn] = lst
		c.orderedSections = append(c.orderedSections, fqn)
	}
	lst.PushBack(section)
	return section
}

func (s *Section) Fqn() string {
	return s.fqn
}

func (s *Section) PrintSection(val bool) {
	s.printSection = val
}
