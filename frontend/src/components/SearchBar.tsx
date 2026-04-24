"use client";

import { Search } from "lucide-react";

interface SearchBarProps {
  value: string;
  scope: string;
  onChange: (q: string) => void;
  onScopeChange: (s: string) => void;
}

export default function SearchBar({ value, scope, onChange, onScopeChange }: SearchBarProps) {
  return (
    <div className="relative flex items-center w-full max-w-2xl">
      <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
        <Search className="h-5 w-5 text-slate-400" />
      </div>
      <input
        type="text"
        className="block w-full pl-10 pr-24 py-2 border border-slate-300 rounded-lg bg-slate-50 placeholder-slate-400 focus:outline-none focus:bg-white focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm transition-all"
        placeholder="Search repositories, users, or organisations…"
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
      <div className="absolute inset-y-0 right-0 flex items-center">
        <select
          value={scope}
          onChange={(e) => onScopeChange(e.target.value)}
          className="h-full py-0 pl-2 pr-7 border-transparent bg-transparent text-slate-500 sm:text-sm rounded-r-md focus:ring-indigo-500 focus:border-indigo-500"
        >
          <option value="all">All</option>
          <option value="repos">Repos</option>
          <option value="users">Users</option>
          <option value="orgs">Orgs</option>
        </select>
      </div>
    </div>
  );
}
