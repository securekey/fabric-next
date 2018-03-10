Welcome to Hyperledger Fabric
=============================

Hyperledger Fabric is a platform for distributed ledger solutions, underpinned
by a modular architecture delivering high degrees of confidentiality, resiliency,
flexibility and scalability.  It is designed to support pluggable implementations
of different components, and accommodate the complexity and intricacies that exist
across the economic ecosystem.

Hyperledger Fabric delivers a uniquely elastic and extensible architecture,
distinguishing it from alternative blockchain solutions. Planning for the
future of enterprise blockchain requires building on top of a fully-vetted,
open source architecture; Hyperledger Fabric is your starting point.

It's recommended for first-time users to begin by going through the
:doc:`getting_started` section in order to gain familiarity with the Hyperledger Fabric
components and the basic transaction flow.  Once comfortable, continue
exploring the library for demos, technical specifications, APIs, etc.

.. note:: If you have questions not addressed by this documentation, or run into
          issues with any of the tutorials, please visit the :doc:`questions`
          page for some tips on where to find additional help.

Before diving in, watch how Hyperledger Fabric is Building a Blockchain for
Business:

.. raw:: html

   <iframe width="560" height="315" src="https://www.youtube.com/embed/EKa5Gh9whgU" frameborder="0" allowfullscreen></iframe>
   <br/><br/>

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   prereqs
   getting_started
   samples

.. toctree::
   :maxdepth: 2
   :caption: Key Concepts

   blockchain
   functionalities
   fabric_model
   identity/identity.md
   membership/membership.md
   ledger
   usecases

.. toctree::
   :maxdepth: 2
   :caption: Tutorials

   build_network
   write_first_app
   channel_update_tutorial
   upgrading_your_network_tutorial
   chaincode
   chaincode4ade
   chaincode4noah
   systemchaincode
   videos

.. toctree::
   :maxdepth: 2
   :caption: Operations Guide

   upgrade_to_one_point_one
   config_update
   msp
   configtx
   endorsement-policies
   error-handling
   logging-control
   enable_tls
   kafka

.. toctree::
   :maxdepth: 2
   :caption: Command Reference

   commands/peercommand.md
   commands/peerchaincode.md
   commands/peerchannel.md
   commands/peerversion.md
   commands/peerlogging.md
   commands/peernode.md
   commands/configtxgen.md
   commands/configtxlator.md
   commands/cryptogen-commands
   commands/fabric-ca-commands

.. toctree::
   :maxdepth: 2
   :caption: Architecture

   arch-deep-dive
   txflow
   Hyperledger Fabric CA's User Guide <http://hyperledger-fabric-ca.readthedocs.io/en/latest>
   fabric-sdks
   channels
   capability_requirements
   couchdb_as_state_database
   peer_event_services
   readwrite
   gossip

.. toctree::
   :maxdepth: 2
   :caption: Troubleshooting and FAQs

   Fabric-FAQ
   ordering-service-faq

.. toctree::
   :maxdepth: 2
   :caption: Contributing

   CONTRIBUTING
   MAINTAINERS
   jira_navigation
   dev-setup/devenv
   dev-setup/build
   Gerrit/lf-account
   Gerrit/gerrit
   Gerrit/changes
   Gerrit/reviewing
   Gerrit/best-practices
   testing
   Style-guides/go-style

.. toctree::
   :maxdepth: 2
   :caption: Appendix

   glossary
   releases
   questions
   status

.. Licensed under Creative Commons Attribution 4.0 International License
   https://creativecommons.org/licenses/by/4.0/
