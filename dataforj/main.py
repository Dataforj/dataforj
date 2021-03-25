import argparse
import dataforj.cli as cli

parser = argparse.ArgumentParser(description='dataforj CLI')
parser.add_argument('command', help='run/unit-test/debug-step/init/add')
parser.add_argument('-e', '--env', help='where the job should be run')
parser.add_argument('-s', '--step', help='name of the step')
parser.add_argument('-t', '--type', help='type of the step')
parser.add_argument('-d', '--dependson', help='steps that this one depends on')
parser.add_argument('-n', '--name', help='name of the project')
parser.add_argument('-p', '--project', help='location of the Dataforj project',
                    default='.')


def main(args):
    args = parser.parse_args()
    print(f'Starting Dataforj ...')
    if args.command == 'unit-test':
        cli.unit_test(args.project, args.env, args.step)
    elif args.command == 'debug-step':
        cli.debug_step(args.project, args.env, args.step)
    elif args.command == 'init':
        cli.init(args.project, args.name)
    elif args.command == 'add':
        cli.add_step(args.project, args.step, args.type, args.dependson)
    elif args.command == 'remove':
        cli.remove_step(args.project, args.step)
    elif args.command == 'run':
        print(f'Running the job on [{args.env}]')
        cli.run(args.project, args.env)
    else:
        raise Exception(f'Unkown command [{args.command}]')


if __name__ == "__main__":
    main()
