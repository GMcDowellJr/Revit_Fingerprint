// Copyright (c) Lookup Foundation and Contributors
// 
// Permission to use, copy, modify, and distribute this software in
// object code form for any purpose and without fee is hereby granted,
// provided that the above copyright notice appears in all copies and
// that both that copyright notice and the limited warranty and
// restricted rights notice below appear in all supporting
// documentation.
// 
// THIS PROGRAM IS PROVIDED "AS IS" AND WITH ALL FAULTS.
// NO IMPLIED WARRANTY OF MERCHANTABILITY OR FITNESS FOR A PARTICULAR USE IS PROVIDED.
// THERE IS NO GUARANTEE THAT THE OPERATION OF THE PROGRAM WILL BE
// UNINTERRUPTED OR ERROR FREE.

using System.Collections;
using LookupEngine.Abstractions.Configuration;
using LookupEngine.Abstractions.Decomposition;

namespace RevitLookup.Core.Decomposition.Descriptors;

public sealed class EnumeratorDescriptor : Descriptor, IDescriptorRedirector
{
    private readonly object? _object;
    private readonly IEnumerator _enumerator;

    public EnumeratorDescriptor(IEnumerator enumerator)
    {
        _enumerator = enumerator;
        try
        {
            _object = enumerator.Current;
        }
        catch
        {
            // ignored
        }
    }

    public bool TryRedirect(string target, out object result)
    {
        if (_object is null)
        {
            result = _enumerator;
            return false;
        }

        result = _object;
        return true;
    }
}